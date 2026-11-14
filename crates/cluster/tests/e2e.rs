//! End-to-end tests: a real control plane and real agents talking over iroh
//! on localhost.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit, SettingsHook, UpgradeConfig};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{CuRates, Metered, Policy},
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput, Tier},
    manage::Selector,
    msg::{
        AccountView, AdminOp, AdminResponse, Invite, JobState, JobView, LedgerKind,
        NodeUpgradeView, Role, RolloutView, TaskState, UpgradeState, Verification,
    },
    net::NetMode,
    receipt::Receipt,
    release::{
        CanaryPhase, CanarySpec, CanaryView, FetchState, Release, ReleaseFile, SignedRelease,
        Target, hex, sha256,
    },
    ticket::JoinTicket,
    ui::{NodeSettings, NodeSettingsView},
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
    // The default node timeout (10 s): a node that is killed closes its
    // connection, which the control plane sees at once, and with 2 s a
    // slow CI runner lost healthy nodes ("node missed heartbeats").
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

async fn agent(name: &str, slots: u32, dir: PathBuf) -> Agent {
    Agent::new(agent_config(name, slots, dir)).await.unwrap()
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

async fn submit(client: &AdminClient, sub: JobSubmission) -> JobId {
    match client.call(AdminOp::Submit(Box::new(sub))).await.unwrap() {
        AdminResponse::Submitted { id } => id,
        other => panic!("unexpected {other:?}"),
    }
}

async fn task_details(client: &AdminClient, id: JobId) -> Vec<pumasi_proto::msg::TaskDetail> {
    match client.call(AdminOp::TaskDetails { id }).await.unwrap() {
        AdminResponse::TaskDetails(d) => d,
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

    // Each task says which node ran it by name, and how long it ran
    // (1.4): the run is the billed time, and WASM has nothing to pull.
    let details = task_details(&client, id).await;
    assert_eq!(details.len(), view.tasks.len());
    let status = cluster.status();
    for (t, d) in view.tasks.iter().zip(&details) {
        assert_eq!(t.index, d.index);
        let node = status
            .nodes
            .iter()
            .find(|n| Some(&n.id) == t.node.as_ref())
            .unwrap();
        assert_eq!(d.node_name.as_deref(), Some(node.info.name.as_str()));
        assert!(["a", "b"].contains(&node.info.name.as_str()));
        assert_eq!(d.run_ms, t.wall_ms);
        assert_eq!(d.pull_ms, 0);
    }

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

/// A laptop's power state, as the availability policy reads it.
struct Battery(std::sync::atomic::AtomicBool);

impl pumasi_agent::availability::Probe for Battery {
    fn on_battery(&self) -> Option<bool> {
        Some(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
    fn idle_time(&self) -> std::result::Result<Duration, String> {
        Ok(Duration::MAX)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_on_battery_takes_no_new_tasks_until_plugged_in() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let battery = std::sync::Arc::new(Battery(true.into()));
    let laptop = Agent::new(AgentConfig {
        availability: Some(pumasi_agent::AvailabilityConfig {
            policy: pumasi_agent::availability::Policy {
                only_on_ac: true,
                ..Default::default()
            },
            probe: battery.clone(),
            interval: Duration::from_millis(100),
        }),
        // The scheduler checks CPUs for image jobs only; WASM needs one.
        max_cpu_percent: Some(1),
        ..agent_config("laptop", 2, tempdir("laptop"))
    })
    .await
    .unwrap();
    assert_eq!(laptop.info().cpus, 1);
    let laptop = spawn_join(laptop, ticket);
    wait_online(&cluster, 1).await;
    // It joined on battery: paused from the start, with the reason.
    let node = cluster.status().nodes.remove(0);
    assert_eq!(
        node.paused.as_deref(),
        Some("on battery power"),
        "{node:#?}"
    );

    let client = admin(&cluster).await;
    let id = submit(&client, submission(vec![vec!["one"], vec!["two"]])).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let AdminResponse::Job(view) = client
        .call(AdminOp::Job {
            id,
            with_output: false,
        })
        .await
        .unwrap()
    else {
        panic!("expected job view");
    };
    assert_eq!(view.summary.state, JobState::Queued, "{view:#?}");

    // Plugged in: the queued tasks run.
    battery.0.store(false, std::sync::atomic::Ordering::SeqCst);
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert!(cluster.status().nodes[0].paused.is_none());

    // Unplugged again: paused again.
    battery.0.store(true, std::sync::atomic::Ordering::SeqCst);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while cluster.status().nodes[0].paused.is_none() {
        assert!(tokio::time::Instant::now() < deadline, "never paused");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await;
    laptop.kill().await;
    cluster.shutdown().await.unwrap();
}

/// A control plane that cannot bind its address lets go of what it had
/// opened, so the next start in the same directory works (on Windows the
/// port of a control plane that just ended can stay taken for a moment).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_that_cannot_bind_leaves_the_data_directory_free() {
    init_tracing();
    let taken = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let dir = tempdir("c");
    let config = || {
        let mut config = ClusterConfig::new(dir.clone());
        config.net = NetMode::Local;
        config.bind = Some(taken.local_addr().unwrap());
        config
    };
    for _ in 0..2 {
        let err = match Cluster::start(config()).await {
            Ok(_) => panic!("started on a taken address"),
            Err(e) => format!("{e:#}"),
        };
        assert!(err.contains("bind"), "{err}");
    }
    let mut free = config();
    free.bind = Some("127.0.0.1:0".parse().unwrap());
    let cluster = Cluster::start(free).await.unwrap();
    cluster.shutdown().await.unwrap();
}

/// A stand-in network: one card on a LAN no test machine is on, and a
/// record of the magic packets asked for.
struct Lan {
    iface: pumasi_proto::power::LanIface,
    sent: std::sync::Mutex<Vec<pumasi_proto::power::Mac>>,
}

impl Lan {
    fn new(host: u8) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            iface: pumasi_proto::power::LanIface {
                name: "en0".into(),
                mac: [0x02, 0, 0, 0, 0, host],
                addr: [198, 18, 77, host],
                prefix: 24,
                wireless: false,
            },
            sent: Default::default(),
        })
    }
}

impl pumasi_proto::power::WakeSender for Lan {
    fn lan(&self) -> Vec<pumasi_proto::power::LanIface> {
        vec![self.iface.clone()]
    }
    fn send(
        &self,
        mac: &pumasi_proto::power::Mac,
        _from: &pumasi_proto::power::LanIface,
    ) -> Result<()> {
        self.sent.lock().unwrap().push(*mac);
        Ok(())
    }
}

async fn node_power(client: &AdminClient, id: &str) -> pumasi_proto::power::NodePowerView {
    match client.call(AdminOp::NodePower).await.unwrap() {
        AdminResponse::NodePower(v) => v.into_iter().find(|n| n.id == id).unwrap(),
        other => panic!("unexpected {other:?}"),
    }
}

/// Release 1.5: a machine that goes to sleep says so first. Its runs move
/// at once (well before the node timeout), it shows as asleep rather than
/// lost, another machine on its LAN sends its magic packet, and it comes
/// back by itself when it wakes. The OS and the network are stand-ins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_machine_that_sleeps_says_goodbye_and_is_woken_over_its_lan() {
    use pumasi_agent::power::{FakePower, PowerEvent};
    init_tracing();
    let mut config = ClusterConfig::new(tempdir("c"));
    config.name = "test-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    // Long, so that only the goodbye can explain a quick requeue.
    config.node_timeout = Duration::from_secs(60);
    let cluster = Cluster::start(config).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();

    let os = std::sync::Arc::new(FakePower::default());
    let laptop_lan = Lan::new(10);
    let laptop = Agent::new(AgentConfig {
        power: pumasi_agent::PowerOptions {
            backend: Some(os.clone()),
            wake_on_lan: true,
            wake: Some(laptop_lan.clone()),
            linger: Duration::from_millis(200),
            ..Default::default()
        },
        ..agent_config("laptop", 2, tempdir("laptop"))
    })
    .await
    .unwrap();
    let laptop = spawn_join(laptop, ticket.clone());
    wait_online(&cluster, 1).await;
    let laptop_id = node_named(&cluster, "laptop").id;
    let client = admin(&cluster).await;

    // It reported what it can do; nothing runs, so nothing is held.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let report = loop {
        if let Some(r) = node_power(&client, &laptop_id).await.report {
            break r;
        }
        assert!(tokio::time::Instant::now() < deadline, "no power report");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        report.stays_awake && report.sleep_notice && report.wake_on_lan,
        "{report:#?}"
    );
    assert_eq!(report.lan, vec![laptop_lan.iface.clone()]);
    assert_eq!(os.held(), 0);

    // While a task runs, the machine is kept awake.
    let id = submit(
        &client,
        submission(vec![vec!["x", "slow"], vec!["y", "slow"]]),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while os.held() == 0 {
        assert!(tokio::time::Instant::now() < deadline, "never kept awake");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The lid closes (the owner's doing: a blocker does not stop that)
    // while both tasks run, with no other machine in the cluster yet.
    let slept = std::time::Instant::now();
    let os2 = os.clone();
    let announce = tokio::task::spawn_blocking(move || os2.announce(PowerEvent::Sleep));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let p = node_power(&client, &laptop_id).await;
        if p.asleep_since_unix.is_some() {
            // Nobody on its LAN to send the packet.
            assert!(!p.can_wake, "{p:#?}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never asleep: {p:#?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    announce.await.unwrap();
    assert!(
        slept.elapsed() < Duration::from_secs(3),
        "the OS was held back for {:?}",
        slept.elapsed()
    );
    assert!(!node_named(&cluster, "laptop").online);

    // Another machine on the same LAN joins. Its report follows the join:
    // the cluster knows only then that it can wake the laptop.
    let desk_lan = Lan::new(11);
    let desk = Agent::new(AgentConfig {
        power: pumasi_agent::PowerOptions {
            wake: Some(desk_lan.clone()),
            ..Default::default()
        },
        ..agent_config("desk", 2, tempdir("desk"))
    })
    .await
    .unwrap();
    let desk = spawn_join(desk, ticket);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !node_power(&client, &laptop_id).await.can_wake {
        assert!(tokio::time::Instant::now() < deadline, "cannot be woken");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let desk_id = node_named(&cluster, "desk").id;

    // The job ends on the other machine, and the move cost no attempt.
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for t in &view.tasks {
        assert_eq!(t.node.as_ref(), Some(&desk_id), "{t:#?}");
        assert_eq!((t.attempts, t.preempted), (2, 1), "{t:#?}");
    }
    let AdminResponse::Events(events) = client
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 100,
        })
        .await
        .unwrap()
    else {
        panic!("expected events");
    };
    assert!(
        events.iter().any(|e| e.kind == "node.asleep"),
        "{events:#?}"
    );
    assert!(
        !events.iter().any(|e| e.kind == "node.offline"),
        "{events:#?}"
    );

    // Waking it by hand: the desk machine, on the same LAN, sends the packet.
    let AdminResponse::Applied(lines) = client
        .call(AdminOp::WakeNode {
            node: "laptop".into(),
        })
        .await
        .unwrap()
    else {
        panic!("expected lines");
    };
    assert!(lines.iter().any(|l| l.contains("desk")), "{lines:?}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while desk_lan.sent.lock().unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "no magic packet");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(desk_lan.sent.lock().unwrap()[0], laptop_lan.iface.mac);
    let p = node_power(&client, &laptop_id).await;
    assert_eq!(p.last_wake_by.as_deref(), Some("desk"), "{p:#?}");

    // It wakes and is back without anyone's help.
    os.announce(PowerEvent::Wake);
    wait_online(&cluster, 2).await;
    assert!(
        node_power(&client, &laptop_id)
            .await
            .asleep_since_unix
            .is_none()
    );
    // An online node is not woken.
    assert!(
        client
            .call(AdminOp::WakeNode {
                node: "laptop".into()
            })
            .await
            .is_err()
    );

    client.close().await;
    laptop.kill().await;
    desk.kill().await;
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
    // `b` is built before the job is submitted: on a busy CI runner that
    // took 3 s, longer than the tasks run (1.5 s), so they finished on `a`
    // before it died and were never rescheduled.
    let b = agent("b", 2, tempdir("b")).await;
    // Two slow tasks: both land on `a`, the only node.
    let id = submit(
        &client,
        submission(vec![vec!["x", "slow"], vec!["y", "slow"]]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let b = spawn_join(b, ticket);
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
        replicas: vec![],
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

/// A node that cannot start a task says so with `FromAgent::Done`, not
/// with a result. Taking that report froze the control plane: it locked
/// its state twice, and every later scheduling step, API call and the
/// shutdown waited for that lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_that_cannot_start_fails_and_the_control_plane_goes_on() {
    init_tracing();
    // A frozen control plane also blocks this runtime's threads, so a
    // tokio timeout would not fire.
    let (finished, wait) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        use std::sync::mpsc::RecvTimeoutError::Timeout;
        if wait.recv_timeout(Duration::from_secs(90)) == Err(Timeout) {
            eprintln!("the control plane froze on a task that could not start");
            std::process::abort();
        }
    });
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_join(agent("a", 1, tempdir("a")).await, ticket);
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;

    // A WASM header and nothing a node can load after it.
    let mut sub = submission(vec![vec!["x"]]);
    sub.module = b"\0asm\x01\0\0\0 not a module".to_vec();
    sub.max_attempts = 1;
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Failed, "{view:#?}");

    // The control plane still schedules, answers and stops.
    let id = submit(&client, submission(vec![vec!["y"]])).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");

    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();
    let _ = finished.send(());
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
        power: Default::default(),
        max_cpu_percent: None,
        availability: None,
        bench: None,
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

/// Rates that make the tiny echo task cost a visible amount: memory so
/// expensive that its few microseconds of reference time cost CU.
fn test_policy(hold_secs: u64, verify_permille: u32, max_strikes: u32) -> Policy {
    Policy {
        rates: CuRates {
            memory_gib_hour: 100_000_000_000_000_000,
            ..CuRates::default()
        },
        hold_secs,
        verify_permille,
        max_strikes,
        ..Policy::default()
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

async fn deposit(client: &AdminClient, name: &str, cu: i64) {
    client
        .call(AdminOp::Deposit {
            account: name.into(),
            amount: cu * 1000,
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
        // Priced from the CU rates by fuel; the provider has no benchmark
        // here, so the reference score.
        assert_eq!(r.billing.cpu_score, 1000);
        let exact = test_policy(1, 0, 3).rates.charge(&Metered {
            executor: Executor::Wasm,
            fuel_used: r.usage.fuel_used,
            wall_ms: r.billing.billed_wall_ms,
            memory_bytes: r.usage.memory_limit_bytes,
            cpu_millis: None,
            cpu_score: 1000,
            gpu_gflops: 0,
            gpu_vram_mib: 0,
        });
        assert_eq!(r.billing.exact_ucu, exact);
        assert!(r.billing.amount > 0);
    }
    // Whole milli-CU move; the remainder carries over to the next run.
    let exact: i64 = rs.iter().map(|r| r.billing.exact_ucu).sum();
    assert_eq!(rs.iter().map(|r| r.billing.amount).sum::<i64>(), cost);
    assert_eq!(cost, exact / 1000);

    let owner = account(&client, "owner").await;
    assert_eq!(owner.balance, 1000 * 1000 - cost);
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
    assert_eq!(owner.balance, 1000 * 1000 - owner.spent);
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
// Phase 7: compute units. The benchmark checks are in `bench.rs`: the
// cluster times CPUs there, which needs a test binary of its own.
// ---------------------------------------------------------------------------

fn node_named(cluster: &Cluster, name: &str) -> pumasi_proto::msg::NodeView {
    cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.info.name == name)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_mode_lends_and_borrows_without_grants() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    set_policy(
        &client,
        Policy {
            ledger: pumasi_proto::economy::LedgerMode::Mutual {
                credit_limit: 1_000_000,
            },
            ..test_policy(0, 0, 3)
        },
    )
    .await;
    // Nobody grants CU in mutual mode.
    assert!(
        client
            .call(AdminOp::Deposit {
                account: "owner".into(),
                amount: 1000,
                memo: String::new(),
            })
            .await
            .is_err()
    );
    // Bob lends his machine: the owner's job runs there on credit.
    let b = spawn_join(
        provider("bobs-pc", 2, tempdir("b"), false).await,
        invite(&cluster, "bob"),
    );
    wait_online(&cluster, 1).await;
    let id = submit(&client, deterministic_echo(3)).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let cost = view.summary.cost;
    assert!(cost > 0);
    // Then bob borrows the owner's machine with what he earned.
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let o = spawn_join(agent("own", 2, tempdir("o")).await, ticket);
    wait_online(&cluster, 2).await;
    b.kill().await;
    let mut sub = deterministic_echo(2);
    sub.account = Some("bob".into());
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let borrowed = view.summary.cost;
    assert!(borrowed > 0);
    // Zero-sum: what one account is down, the other is up.
    let mut total = 0;
    for _ in 0..50 {
        let a = accounts(&client).await;
        if a.iter().all(|a| a.held == 0) {
            total = a.iter().map(|a| a.balance).sum::<i64>();
            let owner = a.iter().find(|a| a.name == "owner").unwrap();
            let bob = a.iter().find(|a| a.name == "bob").unwrap();
            assert_eq!(owner.balance, borrowed - cost);
            assert_eq!(bob.balance, cost - borrowed);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(total, 0);

    client.close().await;
    o.kill().await;
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
        relays: Vec::new(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: Some("docker".into()),
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
    })
    .await
    .unwrap();
    let docker_id = docker_agent.id().to_string();
    let docker_label = format!("label=pumasi.agent={}", docker_agent.id().fmt_short());
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

    // The times of each task (1.4): the pull is told apart from the
    // run, and the run is the billed time.
    for (t, d) in view.tasks.iter().zip(task_details(&client, id).await) {
        assert_eq!(d.node_name.as_deref(), Some("docker"));
        assert_eq!(d.run_ms, t.wall_ms);
        assert!(d.run_ms > 0, "{d:?}");
    }

    // Timeout kills the container; a non-zero exit is reported as such.
    let mut slow = docker_job(&["sleep", "30"], vec![TaskInput::default()]);
    slow.limits.timeout_ms = 1500;
    let id = submit(&client, slow).await;
    let view = wait_job(&client, id, 60).await;
    // The image is on the node by now: nothing was pulled, and the time
    // of the run does not count the wait for one.
    let times = &task_details(&client, id).await[0];
    assert_eq!(times.pull_ms, 0, "{times:?}");
    assert_eq!(times.node_name.as_deref(), Some("docker"));
    assert_eq!(times.run_ms, view.tasks[0].wall_ms);
    assert!((1000..30_000).contains(&times.run_ms), "{times:?}");
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

    // No leftover containers from this agent (other tests run Docker in
    // parallel, so look only at this agent's label).
    let out = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "label=pumasi=1",
            "--filter",
            &docker_label,
        ])
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

// ---------------------------------------------------------------------------
// MicroVM executor (skipped unless PUMASI_TEST_VM_DIR points at a VM
// directory set up with `pumasi vm setup`; CI does this on Linux with KVM)
// ---------------------------------------------------------------------------

fn microvm_config() -> Option<pumasi_agent::MicrovmConfig> {
    let dir = std::env::var_os("PUMASI_TEST_VM_DIR")?;
    let backend = std::env::var("PUMASI_TEST_VM_BACKEND")
        .ok()
        .map(|b| b.parse().expect("PUMASI_TEST_VM_BACKEND"));
    Some(pumasi_agent::MicrovmConfig {
        dir: dir.into(),
        backend,
        confidential: Default::default(),
    })
}

fn microvm_image() -> String {
    std::env::var("PUMASI_TEST_VM_IMAGE").unwrap_or_else(|_| "busybox:1.36".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn microvm_jobs_run_on_an_invited_provider() {
    let Some(vm) = microvm_config() else {
        eprintln!("skipping: PUMASI_TEST_VM_DIR is not set");
        return;
    };
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    // The default rates: a microVM run of a few seconds costs about a
    // milli-CU per second of vCPU.
    set_policy(
        &client,
        Policy {
            hold_secs: 1,
            verify_permille: 0,
            ..Policy::default()
        },
    )
    .await;
    deposit(&client, "owner", 100_000).await;

    // A plain owner machine (WASM only) and a provider that opted into
    // microVMs but was not invited for Docker.
    let owner_ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let wasm_only = spawn_join(agent("wasm-only", 2, tempdir("w")).await, owner_ticket);
    let provider = Agent::new(AgentConfig {
        data_dir: tempdir("p"),
        name: "bobs-pc".into(),
        slots: Some(2),
        net: NetMode::Local,
        relays: Vec::new(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: None,
        microvm: Some(vm),
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
    })
    .await
    .unwrap();
    let provider_id = provider.id().to_string();
    let p = spawn_join(provider, invite(&cluster, "bob"));
    wait_online(&cluster, 2).await;
    let node = cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.id == provider_id)
        .unwrap();
    assert_eq!(node.role, Role::Provider);
    assert_eq!(node.executors, vec![Executor::Wasm, Executor::Microvm]);

    // Shared + per-task input files, an output file, stdout, a fresh
    // writable root per task, its own kernel, and no network.
    let script = "cat shared.txt mine.txt > /output/out.txt && mkdir -p /output/sub && echo $0 > /output/sub/arg.txt \
        && test ! -e /var/leftover && touch /var/leftover \
        && test \"$(uname -r)\" != \"$HOST_KERNEL\" \
        && ! wget -q -T 2 -O /dev/null http://1.1.1.1 && echo isolated";
    let host_kernel = std::process::Command::new("uname")
        .arg("-r")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let tasks = (0..3)
        .map(|i| TaskInput {
            args: vec![format!("arg{i}")],
            env: [("HOST_KERNEL".to_string(), host_kernel.clone())].into(),
            files: vec![FileEntry {
                path: "mine.txt".into(),
                data: format!("task{i}").into_bytes(),
            }],
            ..Default::default()
        })
        .collect();
    let mut sub = docker_job(&["sh", "-c", script], tasks);
    sub.executor = Executor::Microvm;
    sub.docker.as_mut().unwrap().image = microvm_image();
    // Generous: the first task also pulls and unpacks the image.
    sub.limits.timeout_ms = 120_000;
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 600).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(t.node.as_deref(), Some(provider_id.as_str()));
        assert_eq!(t.stdout.as_deref(), Some(&b"isolated\n"[..]), "{t:#?}");
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
    let rs = receipts(&client, id).await;
    assert_eq!(rs.len(), 3);
    for r in &rs {
        r.verify().unwrap();
        assert_eq!(r.usage.executor, Executor::Microvm);
        assert_eq!(r.billing.provider, "bob");
        // A half-second run is a fraction of a milli-CU: priced exactly on
        // the receipt, carried over in the ledger until it adds up.
        assert!(r.billing.exact_ucu > 0, "microvm runs are billed");
    }
    assert_eq!(
        rs.iter().map(|r| r.billing.amount).sum::<i64>(),
        view.summary.cost
    );

    // Timeout kills the VM; exit codes and signals come through.
    let mut slow = docker_job(&["sleep", "60"], vec![TaskInput::default()]);
    slow.executor = Executor::Microvm;
    slow.docker.as_mut().unwrap().image = microvm_image();
    slow.limits.timeout_ms = 3000;
    let id = submit(&client, slow).await;
    let view = wait_job(&client, id, 60).await;
    assert!(
        matches!(
            view.tasks[0].state,
            TaskState::Failed(pumasi_proto::msg::Outcome::Timeout)
        ),
        "{view:#?}"
    );
    let mut fail = docker_job(
        &["sh", "-c", "echo oops >&2; exit 3"],
        vec![TaskInput::default()],
    );
    fail.executor = Executor::Microvm;
    fail.docker.as_mut().unwrap().image = microvm_image();
    let id = submit(&client, fail).await;
    let view = wait_job(&client, id, 60).await;
    assert!(
        matches!(
            view.tasks[0].state,
            TaskState::Failed(pumasi_proto::msg::Outcome::Exited(3))
        ),
        "{view:#?}"
    );
    assert_eq!(view.tasks[0].stderr.as_deref(), Some(&b"oops\n"[..]));

    // A task over its memory limit is reported as out of memory.
    let mut hog = docker_job(
        &["sh", "-c", "x=a; while true; do x=$x$x; done"],
        vec![TaskInput::default()],
    );
    hog.executor = Executor::Microvm;
    hog.docker.as_mut().unwrap().image = microvm_image();
    hog.limits.memory_bytes = 32 << 20;
    let id = submit(&client, hog).await;
    let view = wait_job(&client, id, 60).await;
    match &view.tasks[0].state {
        TaskState::Failed(pumasi_proto::msg::Outcome::Trap(msg)) => {
            assert!(msg.contains("out of memory"), "{msg}")
        }
        other => panic!("expected out of memory, got {other:?}"),
    }

    // Network (phase 9): only with `network = true`, only public
    // addresses, never this host or private ranges. The node's device
    // profile (with its microVM networking) arrives after it is online, so
    // read the node again rather than trust the view from the start.
    let node = cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.id == provider_id)
        .unwrap();
    microvm_network(&client, &node).await;

    client.close().await;
    wasm_only.kill().await;
    p.kill().await;
    cluster.shutdown().await.unwrap();
}

/// Phase 16 with the test TEE (no SEV-SNP or TDX on CI runners): a
/// confidential job's guest attests, the control plane refuses the keys
/// while no admin trusts the measurement, and releases them once it does.
/// The run's input and output cross the node only sealed.
async fn task_executors(client: &AdminClient, id: JobId) -> Vec<Option<Executor>> {
    match client.call(AdminOp::TaskExecutors { id }).await.unwrap() {
        AdminResponse::TaskExecutors(e) => e,
        other => panic!("unexpected {other:?}"),
    }
}

/// An `executor = "image"` job of two tasks, for whichever executor the
/// node has.
fn image_job() -> JobSubmission {
    let tasks = (0..2)
        .map(|i| TaskInput {
            args: vec![format!("arg{i}")],
            ..Default::default()
        })
        .collect();
    let mut sub = docker_job(&["sh", "-c", "cat shared.txt && echo $0"], tasks);
    sub.executor = Executor::Image;
    sub.limits.timeout_ms = 120_000;
    sub
}

/// Release 1.5: an image job on a node with Docker runs in a container.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn image_jobs_run_in_docker_where_the_node_has_it() {
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
        docker: Some("docker".into()),
        ..agent_config("docker", 4, tempdir("d"))
    })
    .await
    .unwrap();
    let docker_id = docker_agent.id().to_string();
    let d = spawn_join(docker_agent, ticket);
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;

    let id = submit(&client, image_job()).await;
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(t.node.as_deref(), Some(docker_id.as_str()));
        assert_eq!(
            t.stdout.as_deref(),
            Some(format!("shared+arg{i}\n").as_bytes()),
            "{t:#?}"
        );
    }
    assert_eq!(
        task_executors(&client, id).await,
        vec![Some(Executor::Docker); 2]
    );
    // The receipts name the executor that ran, which is what is charged.
    for r in receipts(&client, id).await {
        assert_eq!(r.usage.executor, Executor::Docker, "{r:#?}");
    }

    client.close().await;
    wasm_only.kill().await;
    d.kill().await;
    cluster.shutdown().await.unwrap();
}

/// Release 1.5: the same image job on a node without Docker runs in a
/// microVM.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn microvm_runs_an_image_job_on_a_node_without_docker() {
    let Some(vm) = microvm_config() else {
        eprintln!("skipping: PUMASI_TEST_VM_DIR is not set");
        return;
    };
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let wasm_only = spawn_join(agent("wasm-only", 2, tempdir("w")).await, ticket.clone());
    let vm_agent = Agent::new(AgentConfig {
        microvm: Some(vm),
        ..agent_config("vm-only", 2, tempdir("v"))
    })
    .await
    .unwrap();
    let vm_id = vm_agent.id().to_string();
    let v = spawn_join(vm_agent, ticket);
    wait_online(&cluster, 2).await;
    assert_eq!(
        node_named(&cluster, "vm-only").executors,
        vec![Executor::Wasm, Executor::Microvm]
    );
    let client = admin(&cluster).await;

    let mut sub = image_job();
    sub.docker.as_mut().unwrap().image = microvm_image();
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 600).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(t.node.as_deref(), Some(vm_id.as_str()));
        assert_eq!(
            t.stdout.as_deref(),
            Some(format!("shared+arg{i}\n").as_bytes()),
            "{t:#?}"
        );
    }
    assert_eq!(
        task_executors(&client, id).await,
        vec![Some(Executor::Microvm); 2]
    );
    for r in receipts(&client, id).await {
        assert_eq!(r.usage.executor, Executor::Microvm, "{r:#?}");
    }

    client.close().await;
    wasm_only.kill().await;
    v.kill().await;
    cluster.shutdown().await.unwrap();
}

/// Release 1.6: a microVM task's output shows while it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn microvm_output_shows_while_the_task_runs() {
    let Some(vm) = microvm_config() else {
        eprintln!("skipping: PUMASI_TEST_VM_DIR is not set");
        return;
    };
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let vm_agent = Agent::new(AgentConfig {
        microvm: Some(vm),
        ..agent_config("vm-live", 1, tempdir("v"))
    })
    .await
    .unwrap();
    let v = spawn_join(vm_agent, ticket);
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;

    let mut sub = docker_job(
        &[
            "sh",
            "-c",
            "echo started; echo oops >&2; sleep 8; echo done",
        ],
        vec![TaskInput::default()],
    );
    sub.executor = Executor::Microvm;
    sub.docker.as_mut().unwrap().image = microvm_image();
    sub.limits.timeout_ms = 120_000;
    let id = submit(&client, sub).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let mut attempt = 0;
    let mut seen_running = false;
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "no output: {out:?} {err:?}"
        );
        let AdminResponse::Logs(c) = client
            .call(AdminOp::Logs {
                job: id,
                index: 0,
                attempt,
                stdout_from: out.len() as u64,
                stderr_from: err.len() as u64,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        attempt = c.attempt;
        out.extend_from_slice(&c.stdout);
        err.extend_from_slice(&c.stderr);
        if !seen_running && out.starts_with(b"started\n") {
            assert!(
                matches!(c.state, TaskState::Running { .. }),
                "output only came at the end: {:?}",
                c.state
            );
            assert!(c.live);
            seen_running = true;
        }
        if c.done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(seen_running, "never saw output while running: {out:?}");
    assert_eq!(out, b"started\ndone\n");
    assert_eq!(err, b"oops\n");
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(
        view.tasks[0].stdout.as_deref(),
        Some(&b"started\ndone\n"[..])
    );

    client.close().await;
    v.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn microvm_confidential_jobs_get_keys_only_for_a_trusted_measurement() {
    let Some(mut vm) = microvm_config() else {
        eprintln!("skipping: PUMASI_TEST_VM_DIR is not set");
        return;
    };
    init_tracing();
    let mut config = ClusterConfig::new(tempdir("c"));
    config.name = "test-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    config.insecure_test_tee = true;
    let cluster = Cluster::start(config).await.unwrap();
    let client = admin(&cluster).await;
    deposit(&client, "owner", 100_000).await;

    vm.confidential = pumasi_agent::microvm::tee::TeeOptions {
        enabled: false,
        test: true,
    };
    let agent_dir = tempdir("t");
    let mut ac = agent_config("tee-node", 2, agent_dir.clone());
    ac.microvm = Some(vm);
    let agent = Agent::new(ac).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let _a = spawn_join(agent, ticket);
    wait_online(&cluster, 1).await;

    let job = || {
        let mut sub = docker_job(
            &[
                "sh",
                "-c",
                "cat shared.txt mine.txt > /output/out.txt && echo top-$0-secret",
            ],
            vec![TaskInput {
                args: vec!["arg0".into()],
                files: vec![FileEntry {
                    path: "mine.txt".into(),
                    data: b"task-secret".to_vec(),
                }],
                ..Default::default()
            }],
        );
        sub.executor = Executor::Microvm;
        sub.tier = Tier::Confidential;
        sub.max_attempts = 1;
        sub.docker.as_mut().unwrap().image = microvm_image();
        sub.limits.timeout_ms = 180_000;
        sub
    };

    // No admin trusts the guest's measurement yet: no keys.
    let id = submit(&client, job()).await;
    let view = wait_job(&client, id, 600).await;
    assert_eq!(view.summary.state, JobState::Failed, "{view:#?}");
    let TaskState::Failed(pumasi_proto::msg::Outcome::Error(why)) = &view.tasks[0].state else {
        panic!("{view:#?}");
    };
    assert!(why.contains("not trusted"), "{why}");
    let AdminResponse::Confidential(v) = client.call(AdminOp::Confidential).await.unwrap() else {
        panic!("expected the confidential view");
    };
    assert!(v.test_tee);
    assert_eq!(v.measurements.len(), 1, "{v:#?}");
    let m = v.measurements[0].clone();
    assert!(!m.trusted);
    assert_eq!(
        v.nodes[0].tees,
        vec![pumasi_proto::confidential::TeeKind::Test]
    );

    // Trusted: the run gets its keys, and its result is opened.
    let r = client
        .call(AdminOp::TrustMeasurement {
            id: m.id[..12].to_string(),
            trusted: true,
            note: "e2e".into(),
        })
        .await
        .unwrap();
    assert!(matches!(r, AdminResponse::Confidential(_)), "{r:?}");
    let id = submit(&client, job()).await;
    let view = wait_job(&client, id, 600).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(view.tier, Tier::Confidential);
    assert_eq!(
        view.tasks[0].stdout.as_deref(),
        Some(&b"top-arg0-secret\n"[..]),
        "{view:#?}"
    );
    let AdminResponse::Files(files) = client
        .call(AdminOp::TaskFiles { id, index: 0 })
        .await
        .unwrap()
    else {
        panic!("expected files");
    };
    assert_eq!(
        files,
        vec![FileEntry {
            path: "out.txt".into(),
            data: b"shared+task-secret".to_vec(),
        }]
    );
    // Nothing the agent kept holds the input or the output in the clear.
    let mut leaked = Vec::new();
    find_in_files(&agent_dir, b"task-secret", &mut leaked);
    assert!(leaked.is_empty(), "plaintext on the node: {leaked:?}");
}

/// Files under `dir` that contain `needle`.
fn find_in_files(dir: &std::path::Path, needle: &[u8], out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            find_in_files(&p, needle, out);
        } else if let Ok(b) = std::fs::read(&p)
            && b.windows(needle.len()).any(|w| w == needle)
        {
            out.push(p);
        }
    }
}

async fn microvm_network(client: &AdminClient, node: &pumasi_proto::msg::NodeView) {
    let net_job = |script: String| {
        let mut sub = docker_job(&["sh", "-c", &script], vec![TaskInput::default()]);
        sub.executor = Executor::Microvm;
        let d = sub.docker.as_mut().unwrap();
        d.image = microvm_image();
        d.network = true;
        sub.limits.timeout_ms = 120_000;
        sub.max_attempts = 1;
        sub
    };
    if !node.microvm_network {
        // Firecracker without QEMU: network jobs wait for a node that can
        // run them.
        let id = submit(client, net_job("echo never".into())).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let AdminResponse::Job(view) = client
            .call(AdminOp::Job {
                id,
                with_output: false,
            })
            .await
            .unwrap()
        else {
            panic!("expected a job");
        };
        assert_eq!(view.summary.state, JobState::Queued, "{view:#?}");
        assert!(view.tasks.iter().all(|t| t.node.is_none()), "{view:#?}");
        client.call(AdminOp::Cancel { id }).await.unwrap();
        eprintln!("this node's microVMs have no network (no QEMU); network jobs stay pending");
        return;
    }
    // A service on this host's own address, which the guest must not
    // reach (it counts connections).
    let host_ip = std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| {
            s.connect("8.8.8.8:80")?;
            s.local_addr()
        })
        .map(|a| a.ip())
        .expect("this host's address");
    let listener = tokio::net::TcpListener::bind((host_ip, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let reached = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = reached.clone();
    let server = tokio::spawn(async move {
        while let Ok((_s, _)) = listener.accept().await {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let script = format!(
        "wget -q -T 15 -O /dev/null http://example.com/ && echo public-ok; \
         wget -q -T 3 -O /dev/null http://{host_ip}:{port}/ || echo host-blocked; \
         wget -q -T 3 -O /dev/null http://169.254.169.254/ || echo metadata-blocked; \
         wget -q -T 3 -O /dev/null http://10.0.2.2:{port}/ || echo gateway-blocked"
    );
    let id = submit(client, net_job(script)).await;
    let view = wait_job(client, id, 600).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert!(view.network);
    let out = String::from_utf8_lossy(view.tasks[0].stdout.as_deref().unwrap_or_default());
    assert_eq!(
        out, "public-ok\nhost-blocked\nmetadata-blocked\ngateway-blocked\n",
        "{view:#?}"
    );
    assert_eq!(
        reached.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the guest reached this host"
    );
    server.abort();
}

// ---------------------------------------------------------------------------
// Phase 8: private registry images
// ---------------------------------------------------------------------------

/// A private registry for the tests: `PUMASI_TEST_REGISTRY` names an image
/// in it (such as `localhost:5000/private/busybox:1.36`) and
/// `PUMASI_TEST_REGISTRY_AUTH` its login (`user:password`).
fn private_registry() -> Option<(String, String)> {
    Some((
        std::env::var("PUMASI_TEST_REGISTRY").ok()?,
        std::env::var("PUMASI_TEST_REGISTRY_AUTH").ok()?,
    ))
}

async fn set_registry_secret(client: &AdminClient, image: &str, login: &str) {
    let host = image.split('/').next().unwrap();
    let r = client
        .call(AdminOp::SecretSet {
            name: format!("registry/{host}"),
            value: login.into(),
        })
        .await
        .unwrap();
    assert!(matches!(r, AdminResponse::Ok), "{r:?}");
    let AdminResponse::Secrets(list) = client.call(AdminOp::SecretList).await.unwrap() else {
        panic!("expected secrets");
    };
    assert_eq!(
        list.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        vec![format!("registry/{host}")]
    );
}

/// Runs a job with `image` on a node that has `executor`, first without
/// the registry login (refused) and then with it (the control plane pulls
/// the image into the file store and the node loads it from there).
async fn private_image_job(
    executor: Executor,
    image: &str,
    login: &str,
    configure: impl FnOnce(&mut AgentConfig),
) {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let mut config = agent_config("private", 2, tempdir("p"));
    configure(&mut config);
    let node = spawn_join(Agent::new(config).await.unwrap(), ticket);
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;

    let job = |cmd: &str| {
        let mut sub = docker_job(&["sh", "-c", cmd], vec![TaskInput::default()]);
        sub.executor = executor;
        sub.docker.as_mut().unwrap().image = image.into();
        sub.limits.timeout_ms = 120_000;
        sub.max_attempts = 1;
        sub
    };
    let id = submit(&client, job("echo should-not-run")).await;
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Failed, "{view:#?}");

    set_registry_secret(&client, image, login).await;
    let id = submit(&client, job("cat shared.txt; echo private-ok")).await;
    let view = wait_job(&client, id, 600).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(
        view.tasks[0].stdout.as_deref(),
        Some(&b"shared+private-ok\n"[..]),
        "{view:#?}"
    );
    assert_eq!(view.preparing, None);
    // A second job finds the image already on the node.
    let id = submit(&client, job("echo again")).await;
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(view.tasks[0].stdout.as_deref(), Some(&b"again\n"[..]));

    client.close().await;
    node.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docker_runs_an_image_from_a_private_registry() {
    let Some((image, login)) = private_registry() else {
        eprintln!("skipping: PUMASI_TEST_REGISTRY is not set");
        return;
    };
    if !docker_available() {
        eprintln!("skipping: docker is not available");
        return;
    }
    private_image_job(Executor::Docker, &image, &login, |c| {
        c.docker = Some("docker".into())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn microvm_runs_an_image_from_a_private_registry() {
    let Some((image, login)) = private_registry() else {
        eprintln!("skipping: PUMASI_TEST_REGISTRY is not set");
        return;
    };
    let Some(vm) = microvm_config() else {
        eprintln!("skipping: PUMASI_TEST_VM_DIR is not set");
        return;
    };
    private_image_job(Executor::Microvm, &image, &login, |c| c.microvm = Some(vm)).await;
}

// ---------------------------------------------------------------------------
// Phase 4: cluster-driven agent upgrades
// ---------------------------------------------------------------------------

/// An agent whose "binary" is `exe`, a plain file: tests skip running the
/// new binary (`smoke_test: false`) but everything else is real.
async fn upgradable(name: &str, dir: PathBuf, exe: PathBuf, auto: Option<bool>) -> Agent {
    Agent::new(AgentConfig {
        data_dir: dir,
        name: name.into(),
        slots: Some(1),
        net: NetMode::Local,
        relays: Vec::new(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: None,
        microvm: None,
        upgrade: Some(UpgradeConfig {
            exe,
            auto,
            smoke_test: false,
        }),
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
    })
    .await
    .unwrap()
}

fn release_file(bytes: &[u8], version: &str) -> ReleaseFile {
    ReleaseFile {
        target: Target::host(),
        version: version.into(),
        protocol: pumasi_proto::PROTOCOL_VERSION,
        sha256: sha256(bytes),
        size: bytes.len() as u64,
    }
}

async fn upload(client: &AdminClient, bytes: &[u8]) {
    let resp = client
        .call(AdminOp::UploadRelease {
            sha256: sha256(bytes),
            size: bytes.len() as u64,
            offset: 0,
            bytes: bytes.to_vec(),
        })
        .await
        .unwrap();
    assert!(matches!(resp, AdminResponse::Uploaded { have } if have == bytes.len() as u64));
}

async fn rollout_view(client: &AdminClient) -> RolloutView {
    match client.call(AdminOp::RolloutStatus).await.unwrap() {
        AdminResponse::Rollout(v) => v,
        other => panic!("unexpected {other:?}"),
    }
}

async fn wait_upgrade_state(
    client: &AdminClient,
    node: &str,
    pred: impl Fn(&NodeUpgradeView) -> bool,
) -> NodeUpgradeView {
    for _ in 0..300 {
        let view = rollout_view(client).await;
        if let Some(n) = view.nodes.iter().find(|n| n.id == node)
            && pred(n)
        {
            return n.clone();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "node {node} never got there: {:#?}",
        rollout_view(client).await
    );
}

async fn wait_for(what: &str, cond: impl Fn() -> bool) {
    // A minute: slow CI runners run several tests at once.
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn rollout_upgrades_opted_in_agents_and_refuses_forged_binaries() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    let bin = tempdir("bin");
    let old = b"pumasi binary, old build".to_vec();
    let owner_exe = bin.join("owner-pumasi");
    let provider_exe = bin.join("provider-pumasi");
    std::fs::write(&owner_exe, &old).unwrap();
    std::fs::write(&provider_exe, &old).unwrap();

    // One of the owner's machines (accepts upgrades by default) and a
    // provider machine that did not opt in.
    let owner_dir = tempdir("o");
    let owner = upgradable("owner-pc", owner_dir.clone(), owner_exe.clone(), None).await;
    let owner_id = owner.id().to_string();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let owner_run = spawn_join(owner, ticket);
    let provider = upgradable("bobs-pc", tempdir("p"), provider_exe.clone(), None).await;
    let provider_id = provider.id().to_string();
    let provider_run = spawn_join(provider, invite(&cluster, "bob"));
    wait_online(&cluster, 2).await;

    // A rollout of a new build for this platform.
    let new = b"pumasi binary, new build".to_vec();
    upload(&client, &new).await;
    let resp = client
        .call(AdminOp::Rollout {
            files: vec![release_file(&new, "9.9.9-test")],
            force: false,
        })
        .await
        .unwrap();
    let AdminResponse::Rollout(view) = resp else {
        panic!("unexpected {resp:?}");
    };
    // Signed with the cluster key.
    view.releases[0]
        .verify(cluster.addr().id.as_bytes())
        .unwrap();

    // The owner's agent installs it and returns so the process restarts.
    let exit = tokio::time::timeout(Duration::from_secs(30), owner_run.handle)
        .await
        .expect("the owner's agent did not upgrade")
        .unwrap()
        .unwrap();
    assert_eq!(
        exit,
        AgentExit::Upgraded {
            version: "9.9.9-test".into()
        }
    );
    assert_eq!(std::fs::read(&owner_exe).unwrap(), new);
    owner_run.endpoint.close().await;

    // The provider never got it.
    let p = wait_upgrade_state(&client, &provider_id, |n| n.state == UpgradeState::OptedOut).await;
    assert_eq!(p.exe_sha256, Some(sha256(&old)));
    assert_eq!(std::fs::read(&provider_exe).unwrap(), old);

    // "Restart": a new process from the same state directory runs the new
    // binary, is welcomed, and finishes the upgrade.
    let owner = upgradable("owner-pc", owner_dir.clone(), owner_exe.clone(), None).await;
    let owner_endpoint = owner.endpoint().clone();
    let owner_run = tokio::spawn(owner.resume());
    let n = wait_upgrade_state(&client, &owner_id, |n| n.state == UpgradeState::Current).await;
    assert_eq!(n.exe_sha256, Some(sha256(&new)));
    let pending = owner_dir.join("upgrade").join("pending.json");
    wait_for("the upgrade to be committed", || !pending.exists()).await;

    // A release that claims to be this cluster's but is signed by another
    // key is refused before anything is downloaded.
    let forger = iroh::SecretKey::generate();
    let evil = b"pumasi binary, evil build".to_vec();
    upload(&client, &evil).await;
    let forged = SignedRelease::sign(
        Release {
            cluster: *cluster.addr().id.as_bytes(),
            file: release_file(&evil, "6.6.6"),
            created_unix: 0,
        },
        &forger,
    );
    cluster.send_release_for_tests(&owner_id, forged).unwrap();
    let failed = owner_dir.join("upgrade").join("failed.json");
    wait_for("the forged release to be refused", || {
        std::fs::read_to_string(&failed).is_ok_and(|t| t.contains(&hex(&sha256(&evil))))
    })
    .await;
    assert!(
        std::fs::read_to_string(&failed)
            .unwrap()
            .contains("signature")
    );
    assert_eq!(std::fs::read(&owner_exe).unwrap(), new);
    assert!(!owner_run.is_finished());

    // A correctly signed release whose binary was swapped on the control
    // plane's disk: the agent checks the hash and keeps its binary.
    let newer = b"pumasi binary, newer build".to_vec();
    upload(&client, &newer).await;
    let stored = cluster.release_store().path_of(&sha256(&newer));
    let mut tampered = newer.clone();
    tampered[0] ^= 1;
    std::fs::write(&stored, &tampered).unwrap();
    client
        .call(AdminOp::Rollout {
            files: vec![release_file(&newer, "10.0.0-test")],
            force: false,
        })
        .await
        .unwrap();
    let n = wait_upgrade_state(&client, &owner_id, |n| {
        matches!(n.state, UpgradeState::Failed(_))
    })
    .await;
    let UpgradeState::Failed(reason) = n.state else {
        unreachable!()
    };
    assert!(reason.contains("SHA-256"), "{reason}");
    assert_eq!(std::fs::read(&owner_exe).unwrap(), new);
    assert!(!owner_run.is_finished());

    // A rollout older than what a node runs (one upgraded by hand while
    // the rollout was still on) is not offered to it, and the agent refuses
    // it even if it is.
    let older = b"pumasi binary, older build".to_vec();
    upload(&client, &older).await;
    let resp = client
        .call(AdminOp::Rollout {
            files: vec![release_file(&older, "0.0.1")],
            force: false,
        })
        .await
        .unwrap();
    let AdminResponse::Rollout(view) = resp else {
        panic!("unexpected {resp:?}");
    };
    wait_upgrade_state(&client, &owner_id, |n| n.state == UpgradeState::Current).await;
    cluster
        .send_release_for_tests(&owner_id, view.releases[0].clone())
        .unwrap();
    let n = wait_upgrade_state(&client, &owner_id, |n| {
        matches!(n.state, UpgradeState::Failed(_))
    })
    .await;
    let UpgradeState::Failed(reason) = n.state else {
        unreachable!()
    };
    assert!(reason.contains("older"), "{reason}");
    assert_eq!(std::fs::read(&owner_exe).unwrap(), new);
    assert!(!owner_run.is_finished());

    // A binary for another protocol needs --force.
    let mut other = release_file(&newer, "11.0.0");
    other.protocol += 1;
    let err = client
        .call(AdminOp::Rollout {
            files: vec![other],
            force: false,
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("protocol"), "{err}");

    client.call(AdminOp::CancelRollout).await.unwrap();
    owner_run.abort();
    owner_endpoint.close().await;
    provider_run.kill().await;
    client.close().await;
    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Phase 9: device profiles
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 1.7: canary rollouts
// ---------------------------------------------------------------------------

async fn canary_view(client: &AdminClient) -> Option<CanaryView> {
    match client.call(AdminOp::Canary).await.unwrap() {
        AdminResponse::Canary(c) => c.map(|c| *c),
        other => panic!("unexpected {other:?}"),
    }
}

fn canary_spec(nodes: &str) -> Box<CanarySpec> {
    Box::new(CanarySpec {
        nodes: Selector::nodes(nodes).unwrap(),
        soak_secs: 0,
        min_tasks: 0,
        hold: false,
    })
}

#[tokio::test]
async fn canary_upgrades_first_and_the_rest_follow() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    let bin = tempdir("bin");
    let old = b"pumasi binary, old build".to_vec();
    let canary_exe = bin.join("canary-pumasi");
    let rest_exe = bin.join("rest-pumasi");
    std::fs::write(&canary_exe, &old).unwrap();
    std::fs::write(&rest_exe, &old).unwrap();

    let canary_dir = tempdir("a");
    let canary = upgradable("canary-pc", canary_dir.clone(), canary_exe.clone(), None).await;
    let canary_id = canary.id().to_string();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let canary_run = spawn_join(canary, ticket.clone());
    let rest = upgradable("rest-pc", tempdir("b"), rest_exe.clone(), None).await;
    let rest_id = rest.id().to_string();
    let rest_run = spawn_join(rest, ticket);
    wait_online(&cluster, 2).await;

    // A canary that names every node, or none, is refused.
    let new = b"pumasi binary, new build".to_vec();
    upload(&client, &new).await;
    for bad in ["canary-pc,rest-pc", "nobody"] {
        let err = client
            .call(AdminOp::CanaryRollout {
                files: vec![release_file(&new, "9.9.9-test")],
                force: false,
                canary: canary_spec(bad),
            })
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("canar"), "{err:#}");
    }

    client
        .call(AdminOp::CanaryRollout {
            files: vec![release_file(&new, "9.9.9-test")],
            force: false,
            canary: canary_spec("canary-pc"),
        })
        .await
        .unwrap();

    // The canary installs it; the other node waits.
    let exit = tokio::time::timeout(Duration::from_secs(30), canary_run.handle)
        .await
        .expect("the canary did not upgrade")
        .unwrap()
        .unwrap();
    assert_eq!(
        exit,
        AgentExit::Upgraded {
            version: "9.9.9-test".into()
        }
    );
    canary_run.endpoint.close().await;
    let c = canary_view(&client).await.unwrap();
    assert_eq!(c.phase, CanaryPhase::Canary);
    assert_eq!(c.held, vec!["rest-pc".to_string()]);
    assert_eq!(c.canaries[0].id, canary_id);
    wait_upgrade_state(&client, &rest_id, |n| n.state == UpgradeState::Waiting).await;
    assert_eq!(std::fs::read(&rest_exe).unwrap(), old);
    assert!(!rest_run.handle.is_finished());

    // Back on the new binary, the canary passes (no soak, no tasks
    // asked for) and the rest follow.
    let canary = upgradable("canary-pc", canary_dir.clone(), canary_exe.clone(), None).await;
    let _canary_run = tokio::spawn(canary.resume());
    let exit = tokio::time::timeout(Duration::from_secs(30), rest_run.handle)
        .await
        .expect("the rest did not follow the canary")
        .unwrap()
        .unwrap();
    assert_eq!(
        exit,
        AgentExit::Upgraded {
            version: "9.9.9-test".into()
        }
    );
    assert_eq!(std::fs::read(&rest_exe).unwrap(), new);
    let c = canary_view(&client).await.unwrap();
    assert!(matches!(c.phase, CanaryPhase::Promoted { .. }), "{c:?}");
    assert!(c.canaries[0].upgraded_unix.is_some());
    assert!(c.held.is_empty());
}

#[tokio::test]
async fn a_failed_canary_halts_the_rollout_until_an_admin_moves_it() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    let bin = tempdir("bin");
    let old = b"pumasi binary, old build".to_vec();
    let canary_exe = bin.join("canary-pumasi");
    let rest_exe = bin.join("rest-pumasi");
    std::fs::write(&canary_exe, &old).unwrap();
    std::fs::write(&rest_exe, &old).unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let canary = upgradable("canary-pc", tempdir("a"), canary_exe.clone(), None).await;
    let canary_id = canary.id().to_string();
    let canary_run = spawn_join(canary, ticket.clone());
    let rest = upgradable("rest-pc", tempdir("b"), rest_exe.clone(), Some(true)).await;
    let rest_id = rest.id().to_string();
    let rest_run = spawn_join(rest, ticket);
    wait_online(&cluster, 2).await;

    // A release whose stored binary was damaged: the canary refuses it.
    let new = b"pumasi binary, new build".to_vec();
    upload(&client, &new).await;
    let stored = cluster.release_store().path_of(&sha256(&new));
    let mut damaged = new.clone();
    damaged[0] ^= 1;
    std::fs::write(&stored, &damaged).unwrap();
    client
        .call(AdminOp::CanaryRollout {
            files: vec![release_file(&new, "9.9.9-test")],
            force: false,
            canary: canary_spec("canary-pc"),
        })
        .await
        .unwrap();
    wait_upgrade_state(&client, &canary_id, |n| {
        matches!(n.state, UpgradeState::Failed(_))
    })
    .await;
    let mut halted = None;
    for _ in 0..100 {
        let c = canary_view(&client).await.unwrap();
        if let CanaryPhase::Halted { reason } = &c.phase {
            halted = Some(reason.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let reason = halted.expect("the canary never halted");
    assert!(reason.contains("canary-pc"), "{reason}");
    // The rest stay where they are.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let rest = wait_upgrade_state(&client, &rest_id, |_| true).await;
    assert_eq!(rest.state, UpgradeState::Waiting);
    assert_eq!(std::fs::read(&rest_exe).unwrap(), old);

    // The event log says why.
    let AdminResponse::Events(events) = client
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 100,
        })
        .await
        .unwrap()
    else {
        panic!("unexpected response");
    };
    assert!(events.iter().any(|e| e.kind == "rollout.halted"));

    // Cancelling ends the rollout and its canary.
    client.call(AdminOp::CancelRollout).await.unwrap();
    assert!(canary_view(&client).await.is_none());
    assert!(!canary_run.handle.is_finished());
    assert!(!rest_run.handle.is_finished());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_profiles_reach_the_control_plane() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let mut config = agent_config("gpu-box", 2, tempdir("g"));
    config.fake_gpus = Some(vec![pumasi_proto::gpu::GpuInfo {
        index: 0,
        vendor: pumasi_proto::gpu::GpuVendor::Nvidia,
        name: "NVIDIA GeForce RTX 3060".into(),
        uuid: "GPU-test".into(),
        pci: "0000:01:00.0".into(),
        vram_mib: 12 * 1024,
        driver: "570.86.15".into(),
        compute: "8.6".into(),
        display: false,
        fp32_gflops: 12_700,
        measured: true,
        mem_bw_gb_s: 360,
        hash_rate: 0,
        docker: true,
        vfio: false,
        render_node: String::new(),
    }]);
    let node = spawn_join(Agent::new(config).await.unwrap(), ticket);
    wait_online(&cluster, 1).await;
    let mut gpus = Vec::new();
    for _ in 0..100 {
        gpus = node_named(&cluster, "gpu-box").gpus;
        if !gpus.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(gpus.len(), 1, "the signed device profile arrived");
    assert_eq!(gpus[0].info.name, "NVIDIA GeForce RTX 3060");
    assert_eq!(gpus[0].billed_gflops, 12_700, "owner machines are trusted");
    assert_eq!(gpus[0].busy, None);
    assert!(!node_named(&cluster, "gpu-box").microvm_network);
    node.kill().await;
    cluster.shutdown().await.unwrap();
}

/// Triples 256 numbers with a WGSL shader through the `pumasi_webgpu`
/// imports, checks them and prints "ok"; exits 2 without a GPU, 3 when
/// the GPU calls fail (the host's message on stderr), 4 on a wrong result.
fn webgpu_wat() -> String {
    let shader = "@group(0) @binding(0) var<storage, read_write> v: array<u32>;\\n\
@compute @workgroup_size(64) fn main(@builtin(global_invocation_id) id: vec3<u32>) {\\n\
if (id.x < arrayLength(&v)) { v[id.x] = v[id.x] * 3u + 1u; } }";
    let len = shader.replace("\\n", "\n").len();
    format!(
        r#"
(module
  (import "pumasi_webgpu" "adapter" (func $adapter (param i32 i32) (result i32)))
  (import "pumasi_webgpu" "shader" (func $shader (param i32 i32 i32 i32) (result i32)))
  (import "pumasi_webgpu" "buffer" (func $buffer (param i64) (result i32)))
  (import "pumasi_webgpu" "write" (func $write (param i32 i64 i32 i32) (result i32)))
  (import "pumasi_webgpu" "read" (func $read (param i32 i64 i32 i32) (result i32)))
  (import "pumasi_webgpu" "dispatch" (func $dispatch (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "pumasi_webgpu" "error" (func $error (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "{shader}")
  (data (i32.const 4000) "main")
  (data (i32.const 4010) "ok")
  (func $fail (param $code i32)
    (i32.store (i32.const 0) (i32.const 20000))
    (i32.store (i32.const 4) (call $error (i32.const 20000) (i32.const 4000)))
    (drop (call $fd_write (i32.const 2) (i32.const 0) (i32.const 1) (i32.const 8)))
    (call $exit (local.get $code)))
  (func (export "_start")
    (local $sh i32) (local $b i32) (local $i i32)
    (if (i32.lt_s (call $adapter (i32.const 16000) (i32.const 3000)) (i32.const 0)) (then (call $fail (i32.const 2))))
    (local.set $sh (call $shader (i32.const 1024) (i32.const {len}) (i32.const 4000) (i32.const 4)))
    (if (i32.lt_s (local.get $sh) (i32.const 0)) (then (call $fail (i32.const 3))))
    (block $done (loop $fill
      (br_if $done (i32.ge_u (local.get $i) (i32.const 256)))
      (i32.store (i32.add (i32.const 8192) (i32.shl (local.get $i) (i32.const 2))) (local.get $i))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br $fill)))
    (local.set $b (call $buffer (i64.const 1024)))
    (if (i32.lt_s (local.get $b) (i32.const 0)) (then (call $fail (i32.const 3))))
    (if (i32.lt_s (call $write (local.get $b) (i64.const 0) (i32.const 8192) (i32.const 1024)) (i32.const 0))
      (then (call $fail (i32.const 3))))
    (i32.store (i32.const 100) (local.get $b))
    (if (i32.lt_s (call $dispatch (local.get $sh) (i32.const 100) (i32.const 1) (i32.const 4) (i32.const 1) (i32.const 1)) (i32.const 0))
      (then (call $fail (i32.const 3))))
    (if (i32.lt_s (call $read (local.get $b) (i64.const 0) (i32.const 12288) (i32.const 1024)) (i32.const 0))
      (then (call $fail (i32.const 3))))
    (local.set $i (i32.const 0))
    (block $ok (loop $check
      (br_if $ok (i32.ge_u (local.get $i) (i32.const 256)))
      (if (i32.ne
            (i32.load (i32.add (i32.const 12288) (i32.shl (local.get $i) (i32.const 2))))
            (i32.add (i32.mul (local.get $i) (i32.const 3)) (i32.const 1)))
        (then (call $exit (i32.const 4))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br $check)))
    (i32.store (i32.const 0) (i32.const 4010))
    (i32.store (i32.const 4) (i32.const 2))
    (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8)))))
"#
    )
}

/// A wasm job with `gpus = 1` runs only on the node that offers a WebGPU
/// adapter, one task at a time per adapter, and its receipts name the
/// adapter. Uses a software rasterizer when the machine has no GPU
/// (`PUMASI_TEST_WEBGPU=require` makes a missing adapter a failure).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webgpu_jobs_run_on_nodes_that_offer_a_gpu() {
    init_tracing();
    let mut config = agent_config("gpu", 4, tempdir("gpu"));
    config.webgpu = Some(pumasi_agent::webgpu::Options {
        software: true,
        bench: true,
    });
    let gpu_agent = Agent::new(config).await.unwrap();
    let adapters = gpu_agent.webgpu().profile.adapters.clone();
    if adapters.is_empty() {
        assert!(
            std::env::var("PUMASI_TEST_WEBGPU").as_deref() != Ok("require"),
            "no WebGPU adapter: {:?}",
            gpu_agent.webgpu().profile.notes
        );
        eprintln!("skipped: no WebGPU adapter here");
        return;
    }
    eprintln!("WebGPU adapter: {}", adapters[0].describe());
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let g = spawn_join(gpu_agent, ticket.clone());
    let p = spawn_join(agent("plain", 4, tempdir("plain")).await, ticket);
    wait_online(&cluster, 2).await;
    for _ in 0..100 {
        if !node_named(&cluster, "gpu").webgpu.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let node = node_named(&cluster, "gpu");
    assert_eq!(node.webgpu.len(), adapters.len());
    assert!(node_named(&cluster, "plain").webgpu.is_empty());

    let client = admin(&cluster).await;
    let mut sub = submission(vec![vec![]; 4]);
    sub.name = "webgpu".into();
    sub.module = wat::parse_str(webgpu_wat()).unwrap();
    sub.gpu = Some(pumasi_proto::gpu::GpuRequest {
        count: 1,
        vendor: None,
        min_vram_mib: 0,
    });
    sub.max_attempts = 1;
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 120).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for t in &view.tasks {
        assert_eq!(t.stdout.as_deref(), Some(&b"ok"[..]));
        assert_eq!(
            t.node.as_deref(),
            Some(node.id.as_str()),
            "only the GPU node"
        );
        let err = String::from_utf8_lossy(t.stderr.as_deref().unwrap_or_default());
        assert!(err.contains("[pumasi] WebGPU"), "{err}");
    }
    let r = receipts(&client, id).await;
    assert_eq!(r.len(), 4);
    for r in &r {
        r.verify().unwrap();
        let g = r.gpu.as_ref().expect("GPU billing on the receipt");
        assert!(g.names[0].contains("WebGPU"), "{g:?}");
    }

    // Without an adapter the same module fails politely (exit 2).
    let mut plain = submission(vec![vec![]]);
    plain.module = wat::parse_str(webgpu_wat()).unwrap();
    plain.max_attempts = 1;
    let id = submit(&client, plain).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Failed);

    client.close().await;
    g.kill().await;
    p.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn nodes_keep_the_name_an_admin_gave_them() {
    init_tracing();
    let dir = tempdir("rename");
    let cluster = start_cluster(dir.clone()).await.unwrap();
    let client = admin(&cluster).await;
    let a_dir = tempdir("ra");
    let a = agent("pc-a", 1, a_dir.clone()).await;
    let a_id = a.id().to_string();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a_run = spawn_join(a, ticket.clone());
    let b = agent("pc-b", 1, tempdir("rb")).await;
    let b_id = b.id().to_string();
    let _b_run = spawn_join(b, ticket);
    wait_online(&cluster, 2).await;
    let name_of = |id: &str| {
        cluster
            .status()
            .nodes
            .into_iter()
            .find(|n| n.id == id)
            .map(|n| (n.info.name, n.labels.get("name").cloned()))
            .unwrap()
    };

    // By the agent's name; then usable by the new one everywhere.
    client
        .call(AdminOp::RenameNode {
            node: "pc-a".into(),
            name: Some("거실 데스크톱".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        name_of(&a_id),
        (
            "거실 데스크톱".into(),
            Some(vec!["거실 데스크톱".to_string()])
        )
    );
    client
        .call(AdminOp::EditNode {
            node: "거실 데스크톱".into(),
            edit: Box::new(pumasi_proto::manage::NodeEdit {
                cordon: Some(true),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    assert!(
        cluster
            .status()
            .nodes
            .iter()
            .any(|n| n.id == a_id && n.config.cordoned)
    );

    // Names stay unique, and bad ones are refused.
    let err = client
        .call(AdminOp::RenameNode {
            node: b_id[..10].into(),
            name: Some("거실 데스크톱".into()),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already named"), "{err}");
    let err = client
        .call(AdminOp::RenameNode {
            node: "pc-b".into(),
            name: Some("x".repeat(65)),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("at most 64"), "{err}");

    // The agent reconnecting with its own name keeps the admin's.
    a_run.kill().await;
    let a = agent("pc-a", 1, a_dir.clone()).await;
    let a_run = RunningAgent {
        endpoint: a.endpoint().clone(),
        handle: tokio::spawn(a.resume()),
    };
    for _ in 0..100 {
        if cluster
            .status()
            .nodes
            .iter()
            .any(|n| n.id == a_id && n.online)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(name_of(&a_id).0, "거실 데스크톱");

    // It survives a control plane restart.
    client.close().await;
    a_run.kill().await;
    cluster.shutdown().await.unwrap();
    let cluster = start_cluster(dir).await.unwrap();
    let client = admin(&cluster).await;
    let named = |c: &Cluster, id: &str| {
        c.status()
            .nodes
            .into_iter()
            .find(|n| n.id == id)
            .map(|n| n.info.name)
    };
    assert_eq!(named(&cluster, &a_id).as_deref(), Some("거실 데스크톱"));

    // Back to the agent's name, even while it is offline.
    client
        .call(AdminOp::RenameNode {
            node: "거실 데스크톱".into(),
            name: None,
        })
        .await
        .unwrap();
    assert_eq!(named(&cluster, &a_id).as_deref(), Some("pc-a"));

    // remove-node takes a name too.
    client
        .call(AdminOp::RemoveNode { id: "pc-b".into() })
        .await
        .unwrap();
    assert_eq!(named(&cluster, &b_id), None);
    client.close().await;
}

/// A small stand-in for a release binary of `version` for this platform:
/// this test binary's executable header, then a build marker.
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

/// A version newer than this build's, of the same length.
fn newer_version() -> String {
    let mine = pumasi_proto::version();
    let core_end = mine.find(['-', '+']).unwrap_or(mine.len());
    let core: String = mine[..core_end]
        .chars()
        .map(|c| if c.is_ascii_digit() { '9' } else { c })
        .collect();
    format!("{core}{}", &mine[core_end..])
}

#[tokio::test]
async fn upgrades_to_the_newest_published_release() {
    use axum::{
        Router,
        extract::{Path, State},
        http::{StatusCode, header},
        response::IntoResponse,
        routing::get,
    };
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    init_tracing();

    let version = newer_version();
    let tag = format!("v{version}");
    let good = binary_claiming(&version);
    let asset = pumasi_proto::release::asset_name(&Target::host());

    // A release site: v<newer> is right; v<newer>-bad lists another hash.
    type Files = Arc<Mutex<HashMap<String, Vec<u8>>>>;
    let files: Files = Default::default();
    let sums = |bin: &[u8]| {
        format!(
            "{}  {asset}\n{}  pumasi-plan9-mips.exe\n",
            hex(&sha256(bin)),
            hex(&sha256(b"other"))
        )
    };
    {
        let mut f = files.lock().unwrap();
        f.insert(format!("{tag}/SHA256SUMS"), sums(&good).into_bytes());
        f.insert(format!("{tag}/{asset}"), good.clone());
        let bad = format!("v{}", newer_version().replacen('9', "8", 1));
        f.insert(
            format!("{bad}/SHA256SUMS"),
            sums(b"something else").into_bytes(),
        );
        f.insert(format!("{bad}/{asset}"), good.clone());
    }
    let latest_tag = tag.clone();
    let app = Router::new()
        .route(
            "/releases/latest",
            get(move || {
                let tag = latest_tag.clone();
                async move {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, format!("/releases/tag/{tag}"))],
                    )
                }
            }),
        )
        .route(
            "/releases/download/{tag}/{file}",
            get(
                |State(files): State<Files>, Path((tag, file)): Path<(String, String)>| async move {
                    match files.lock().unwrap().get(&format!("{tag}/{file}")) {
                        Some(b) => b.clone().into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                },
            ),
        )
        .with_state(files.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let site = format!("http://{}/releases", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut config = ClusterConfig::new(tempdir("latest"));
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    config.release_source = site.clone();
    let cluster = Cluster::start(config).await.unwrap();
    let client = admin(&cluster).await;

    let exe = tempdir("latest-bin").join("pumasi");
    std::fs::write(&exe, b"pumasi binary, old build").unwrap();
    let node = upgradable("owner-pc", tempdir("lo"), exe.clone(), None).await;
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let run = spawn_join(node, ticket);
    wait_online(&cluster, 1).await;

    let latest = |check| {
        let client = &client;
        async move {
            match client.call(AdminOp::LatestRelease { check }).await.unwrap() {
                AdminResponse::LatestRelease(v) => *v,
                other => panic!("unexpected {other:?}"),
            }
        }
    };
    let v = latest(true).await;
    assert_eq!(v.latest.as_deref(), Some(tag.as_str()));
    assert_eq!(v.source, site);
    assert_eq!(v.control_plane, pumasi_proto::version());

    let settle = || async {
        for _ in 0..300 {
            let v = latest(false).await;
            if !matches!(v.fetch, FetchState::Running { .. }) {
                return v.fetch;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the download never finished");
    };

    // A release whose binary does not match its SHA256SUMS is refused and
    // nothing is rolled out.
    let bad = format!("v{}", newer_version().replacen('9', "8", 1));
    client
        .call(AdminOp::UpgradeToLatest {
            version: Some(bad.clone()),
            force: false,
        })
        .await
        .unwrap();
    let FetchState::Failed {
        reason,
        version: failed,
        ..
    } = settle().await
    else {
        panic!("a tampered release was accepted");
    };
    assert_eq!(failed.as_deref(), Some(bad.as_str()));
    assert!(reason.contains("does not match SHA256SUMS"), "{reason}");
    assert!(rollout_view(&client).await.releases.is_empty());
    // A tag that is not one never reaches a URL.
    assert!(
        client
            .call(AdminOp::UpgradeToLatest {
                version: Some("../../etc".into()),
                force: false,
            })
            .await
            .is_err()
    );

    // The newest release: downloaded, checked, signed and rolled out.
    client
        .call(AdminOp::UpgradeToLatest {
            version: None,
            force: false,
        })
        .await
        .unwrap();
    let FetchState::Done {
        version: v,
        summary,
        ..
    } = settle().await
    else {
        panic!("the upgrade failed: {:?}", latest(false).await.fetch);
    };
    assert_eq!(v, tag);
    assert!(summary.contains(&Target::host().to_string()), "{summary}");
    let view = rollout_view(&client).await;
    assert_eq!(view.releases.len(), 1);
    assert_eq!(view.releases[0].file().version, version);
    assert_eq!(view.releases[0].file().sha256, sha256(&good));
    view.releases[0]
        .verify(cluster.addr().id.as_bytes())
        .unwrap();
    // The control plane leaves its own binary to a member agent on its
    // machine, and installs it itself without one.
    let node_id = cluster.status().nodes[0].id.clone();
    assert!(cluster.own_upgrade(Some(&node_id)).is_none());
    let own = cluster
        .own_upgrade(None)
        .expect("no binary for the control plane");
    assert_eq!(own.file.version, version);
    assert_eq!(std::fs::read(&own.path).unwrap(), good);

    let exit = tokio::time::timeout(Duration::from_secs(60), run.handle)
        .await
        .expect("the agent did not upgrade")
        .unwrap()
        .unwrap();
    assert_eq!(exit, AgentExit::Upgraded { version });
    assert_eq!(std::fs::read(&exe).unwrap(), good);
    run.endpoint.close().await;
    client.close().await;
}

/// A control plane whose nodes are all current, or that has none, still
/// fetches the release for its own platform when it is behind, to install
/// it over its own binary.
#[tokio::test]
async fn a_control_plane_without_agents_upgrades_itself() {
    use axum::{
        Router,
        extract::Path,
        http::{StatusCode, header},
        response::IntoResponse,
        routing::get,
    };
    init_tracing();

    let version = newer_version();
    let tag = format!("v{version}");
    let good = binary_claiming(&version);
    let asset = pumasi_proto::release::asset_name(&Target::host());
    let sums = format!("{}  {asset}\n", hex(&sha256(&good)));
    let latest_tag = tag.clone();
    let (dl_tag, dl_asset, dl_good) = (tag.clone(), asset.clone(), good.clone());
    let app = Router::new()
        .route(
            "/releases/latest",
            get(move || {
                let tag = latest_tag.clone();
                async move {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, format!("/releases/tag/{tag}"))],
                    )
                }
            }),
        )
        .route(
            "/releases/download/{tag}/{file}",
            get(move |Path((t, file)): Path<(String, String)>| {
                let ok = t == dl_tag;
                let body = if ok && file == "SHA256SUMS" {
                    Some(sums.clone().into_bytes())
                } else if ok && file == dl_asset {
                    Some(dl_good.clone())
                } else {
                    None
                };
                async move {
                    match body {
                        Some(b) => b.into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let site = format!("http://{}/releases", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut config = ClusterConfig::new(tempdir("own-upgrade"));
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.release_source = site;
    let cluster = Cluster::start(config).await.unwrap();
    let client = admin(&cluster).await;
    assert!(cluster.own_upgrade(None).is_none());

    client
        .call(AdminOp::UpgradeToLatest {
            version: None,
            force: false,
        })
        .await
        .unwrap();
    let mut fetch = FetchState::Idle;
    for _ in 0..300 {
        fetch = match client
            .call(AdminOp::LatestRelease { check: false })
            .await
            .unwrap()
        {
            AdminResponse::LatestRelease(v) => v.fetch,
            other => panic!("unexpected {other:?}"),
        };
        if !matches!(fetch, FetchState::Running { .. } | FetchState::Idle) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let FetchState::Done { summary, .. } = fetch else {
        panic!("the upgrade did not start: {fetch:?}");
    };
    assert!(summary.contains(&Target::host().to_string()), "{summary}");
    assert!(cluster.rollout_settled());
    let own = cluster
        .own_upgrade(None)
        .expect("no binary for the control plane");
    assert_eq!(own.file.version, version);
    assert_eq!(std::fs::read(&own.path).unwrap(), good);
    client.close().await;
}

// ---------------------------------------------------------------------------
// 1.7: a node's availability settings changed from the dashboard
// ---------------------------------------------------------------------------

async fn node_settings(client: &AdminClient, node: &str) -> NodeSettingsView {
    match client
        .call(AdminOp::NodeSettings { node: node.into() })
        .await
        .unwrap()
    {
        AdminResponse::NodeSettings(v) => *v,
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn node_settings_are_written_and_the_agent_restarts_with_them() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();

    // One agent run as a service (it has an options file), one by hand.
    let file = tempdir("svc").join("agent.toml");
    let written = std::sync::Arc::new(std::sync::Mutex::new(None::<NodeSettings>));
    let w = written.clone();
    let path = file.clone();
    let svc = agent("svc-pc", 2, tempdir("a"))
        .await
        .with_settings_hook(SettingsHook {
            file: file.clone(),
            write: std::sync::Arc::new(move |s: &NodeSettings| {
                std::fs::write(&path, s.changes().join(" "))?;
                *w.lock().unwrap() = Some(s.clone());
                Ok(())
            }),
        });
    let svc_run = spawn_join(svc, ticket.clone());
    let manual_run = spawn_join(agent("hand-pc", 1, tempdir("b")).await, ticket);
    wait_online(&cluster, 2).await;

    // Both say what they run with.
    let mut v = node_settings(&client, "svc-pc").await;
    for _ in 0..50 {
        if v.report.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        v = node_settings(&client, "svc-pc").await;
    }
    let report = v.report.expect("no settings report");
    assert_eq!(report.settings.slots, Some(2));
    assert_eq!(report.file.as_deref(), Some(&*file.display().to_string()));
    assert!(!report.restarting);

    // Bad settings and an agent started by hand are refused.
    let bad = NodeSettings {
        hours: "25:00-07:00".into(),
        ..Default::default()
    };
    assert!(
        client
            .call(AdminOp::SetNodeSettings {
                node: "svc-pc".into(),
                settings: Box::new(bad),
            })
            .await
            .is_err()
    );
    let mut refused = None;
    for _ in 0..50 {
        match client
            .call(AdminOp::SetNodeSettings {
                node: "hand-pc".into(),
                settings: Box::new(NodeSettings::default()),
            })
            .await
        {
            Err(e) => {
                refused = Some(format!("{e:#}"));
                if refused.as_deref().is_some_and(|e| e.contains("by hand")) {
                    break;
                }
            }
            Ok(r) => panic!("a hand-started agent took settings: {r:?}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(refused.unwrap().contains("by hand"));

    // New settings: written to the file, then the idle agent restarts.
    let new = NodeSettings {
        slots: Some(1),
        max_cpu_percent: Some(50),
        only_on_ac: true,
        idle_minutes: Some(5),
        hours: "22:00-07:00".into(),
    };
    client
        .call(AdminOp::SetNodeSettings {
            node: "svc-pc".into(),
            settings: Box::new(new.clone()),
        })
        .await
        .unwrap();
    assert_eq!(written.lock().unwrap().as_ref(), Some(&new));
    let text = std::fs::read_to_string(&file).unwrap();
    assert!(text.contains("--hours 22:00-07:00"), "{text}");
    let exit = tokio::time::timeout(Duration::from_secs(30), svc_run.handle)
        .await
        .expect("the agent did not restart")
        .unwrap()
        .unwrap();
    assert_eq!(exit, AgentExit::Reconfigured);
    assert!(!manual_run.handle.is_finished());

    // The audit log and the events say who changed what.
    let AdminResponse::Events(events) = client
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 100,
        })
        .await
        .unwrap()
    else {
        panic!("unexpected response");
    };
    assert!(
        events.iter().any(|e| e.kind == "node.settings"),
        "{events:?}"
    );
}
