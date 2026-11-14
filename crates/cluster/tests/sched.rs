//! End-to-end tests of 1.9 scheduling over real iroh connections: gangs,
//! spreading and affinity, CPU and memory quotas, schedules in a time zone,
//! and checkpoints: a checkpointing task moved off a drained node finishes
//! on another without starting over (WASM, and Docker when it is there).

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    job::{DockerSpec, Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    manage::{NodeEdit, ScheduleSpec},
    msg::{AdminOp, AdminResponse, JobState, JobView, TaskState},
    net::NetMode,
    sched::{Checkpoint, Domain, PlacementView, ResourceQuota, Spread},
    ticket::JoinTicket,
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

/// Counts to 40, one step every 100 ms, keeping the count in
/// /checkpoint/n (written to n.tmp, then renamed). Starts from the count it
/// finds there, and prints "resumed" when that was not 0, else "fresh".
/// /checkpoint is the third preopened directory (fd 5).
const COUNTER_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "path_open" (func $open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_read" (func $read (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_close" (func $close (param i32) (result i32)))
  (import "wasi_snapshot_preview1" "path_rename" (func $rename (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "clock_time_get" (func $now (param i32 i64 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 400) "n")
  (data (i32.const 410) "n.tmp")
  (data (i32.const 420) "resumed\n")
  (data (i32.const 430) "fresh\n")
  (func $time (result i64)
    (drop (call $now (i32.const 1) (i64.const 1000) (i32.const 320)))
    (i64.load (i32.const 320)))
  (func $sleep (param $ns i64) (local $t i64)
    (local.set $t (call $time))
    (block $d (loop $l
      (br_if $d (i64.ge_u (i64.sub (call $time) (local.get $t)) (local.get $ns)))
      (br $l))))
  (func $save
    (if (i32.eqz (call $open (i32.const 5) (i32.const 0) (i32.const 410) (i32.const 5)
          (i32.const 9) (i64.const 0x1fffffff) (i64.const 0x1fffffff) (i32.const 0) (i32.const 300)))
      (then
        (i32.store (i32.const 100) (i32.const 200))
        (i32.store (i32.const 104) (i32.const 8))
        (drop (call $write (i32.load (i32.const 300)) (i32.const 100) (i32.const 1) (i32.const 108)))
        (drop (call $close (i32.load (i32.const 300))))
        (drop (call $rename (i32.const 5) (i32.const 410) (i32.const 5) (i32.const 5) (i32.const 400) (i32.const 1))))))
  (func (export "_start") (local $start i64) (local $resumed i32)
    (i64.store (i32.const 200) (i64.const 0))
    (if (i32.eqz (call $open (i32.const 5) (i32.const 0) (i32.const 400) (i32.const 1)
          (i32.const 0) (i64.const 0x1fffffff) (i64.const 0x1fffffff) (i32.const 0) (i32.const 300)))
      (then
        (i32.store (i32.const 100) (i32.const 200))
        (i32.store (i32.const 104) (i32.const 8))
        (drop (call $read (i32.load (i32.const 300)) (i32.const 100) (i32.const 1) (i32.const 108)))
        (drop (call $close (i32.load (i32.const 300))))))
    (local.set $start (i64.load (i32.const 200)))
    (block $done (loop $step
      (br_if $done (i64.ge_u (i64.load (i32.const 200)) (i64.const 40)))
      (call $sleep (i64.const 100000000))
      (i64.store (i32.const 200) (i64.add (i64.load (i32.const 200)) (i64.const 1)))
      (call $save)
      (br $step)))
    (local.set $resumed (i64.gt_u (local.get $start) (i64.const 0)))
    (i32.store (i32.const 100) (select (i32.const 420) (i32.const 430) (local.get $resumed)))
    (i32.store (i32.const 104) (select (i32.const 8) (i32.const 6) (local.get $resumed)))
    (drop (call $write (i32.const 1) (i32.const 100) (i32.const 1) (i32.const 108)))))
"#;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-sched-{tag}-{}", rand_u64()));
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
    // The default node timeout (10 s), not the 2 s of the test files that
    // lose nodes: no test here does. On a Windows runner where every call
    // took over a second for a while, healthy nodes went 2 s without a
    // heartbeat read and had their runs rescheduled.
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
    spawn_agent_with(name, slots, ticket, None).await
}

async fn spawn_agent_with(
    name: &str,
    slots: u32,
    ticket: JoinTicket,
    docker: Option<String>,
) -> RunningAgent {
    let config = AgentConfig {
        data_dir: tempdir(name),
        name: name.into(),
        slots: Some(slots),
        net: NetMode::Local,
        relays: Vec::new(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker,
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
        placement: Default::default(),
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
        queue_timeout_ms: None,
        gpu_capabilities: vec![],
    }
}

async fn submit(client: &AdminClient, sub: JobSubmission) -> JobId {
    // With its placement (1.9).
    match client.call(AdminOp::submit(sub)).await.unwrap() {
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

async fn edit(client: &AdminClient, node: &str, edit: NodeEdit) {
    client
        .call(AdminOp::EditNode {
            node: node.into(),
            edit: Box::new(edit),
        })
        .await
        .unwrap();
}

async fn placement(client: &AdminClient, id: JobId) -> PlacementView {
    let AdminResponse::Placement(p) = client.call(AdminOp::Placement { id }).await.unwrap() else {
        panic!("expected a placement view");
    };
    *p
}

fn node_of(cluster: &Cluster, view: &JobView, index: usize) -> String {
    let id = view.tasks[index].node.clone().expect("ran somewhere");
    cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.id == id)
        .map(|n| n.info.name)
        .unwrap()
}

fn uncordon() -> NodeEdit {
    NodeEdit {
        cordon: Some(false),
        ..Default::default()
    }
}

fn cordon() -> NodeEdit {
    NodeEdit {
        cordon: Some(true),
        ..Default::default()
    }
}

fn counter_job() -> JobSubmission {
    let mut sub = submission(vec![vec!["count"]]);
    sub.name = "counter".into();
    sub.module = wat::parse_str(COUNTER_WAT).unwrap();
    sub.max_attempts = 1;
    sub.placement.checkpoint = Some(Checkpoint { grace_ms: 5_000 });
    sub
}

/// The release gate: a checkpointing task moved off a drained node
/// finishes on another without starting over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpointed_task_moves_off_a_drained_node_without_starting_over() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", 1, ticket.clone()).await;
    let b = spawn_agent("b", 1, ticket).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    edit(&client, "b", cordon()).await;

    let id = submit(&client, counter_job()).await;
    wait_running(&client, id, 1).await;
    // A dozen steps saved on a.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    edit(&client, "b", uncordon()).await;
    let AdminResponse::Drained { moved } = client
        .call(AdminOp::Drain { node: "a".into() })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(moved, 1);

    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let t = &view.tasks[0];
    assert_eq!(
        String::from_utf8_lossy(t.stdout.as_deref().unwrap_or_default()),
        "resumed\n",
        "it went on from its checkpoint: {view:#?}"
    );
    assert_eq!(node_of(&cluster, &view, 0), "b");
    // Moved, not failed: max_attempts = 1 still held.
    assert_eq!(t.preempted, 1);
    let p = placement(&client, id).await;
    assert_eq!(p.checkpoints.len(), 1, "{p:#?}");
    assert_eq!(p.checkpoints[0].resumed, 1);
    assert!(p.checkpoints[0].size == 8, "{p:#?}");
    assert!(p.stopping.is_empty());

    // Without a drain it runs through from the start.
    let id = submit(&client, counter_job()).await;
    let view = wait_job(&client, id, 60).await;
    assert_eq!(
        String::from_utf8_lossy(view.tasks[0].stdout.as_deref().unwrap_or_default()),
        "fresh\n"
    );

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gangs_wait_for_room_for_all_their_tasks() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", 1, ticket.clone()).await;
    let b = spawn_agent("b", 1, ticket.clone()).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;

    let mut sub = submission(vec![
        vec!["0", "slow"],
        vec!["1", "slow"],
        vec!["2", "slow"],
    ]);
    sub.placement.gang = true;
    let id = submit(&client, sub).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let view = job(&client, id).await;
    assert!(
        view.tasks.iter().all(|t| t.state == TaskState::Pending),
        "two slots for three tasks: none starts: {view:#?}"
    );
    let p = placement(&client, id).await;
    assert!(p.placement.gang);
    assert!(
        p.waiting.as_deref().unwrap_or("").contains("3 tasks"),
        "{p:#?}"
    );

    // A third machine: all three start together.
    let c = spawn_agent("c", 1, ticket).await;
    wait_online(&cluster, 3).await;
    let view = wait_running(&client, id, 3).await;
    let mut nodes: Vec<String> = (0..3).map(|i| node_of(&cluster, &view, i)).collect();
    nodes.sort();
    assert_eq!(nodes, vec!["a", "b", "c"]);
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");

    client.close().await;
    for ag in [a, b, c] {
        ag.kill().await;
    }
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spread_affinity_and_cpu_quotas() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", 4, ticket.clone()).await;
    let b = spawn_agent("b", 4, ticket).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    for (n, zone) in [("a", "x"), ("b", "y")] {
        edit(
            &client,
            n,
            NodeEdit {
                set_labels: [("zone".to_string(), zone.to_string())].into(),
                ..Default::default()
            },
        )
        .await;
    }

    // At most one task per zone: two run, one on each, and the third
    // waits for one of them.
    let mut sub = submission(vec![
        vec!["0", "slow"],
        vec!["1", "slow"],
        vec!["2", "slow"],
    ]);
    sub.placement.spread = Some(Spread {
        by: Domain::Label("zone".into()),
        max: Some(1),
    });
    let spread = submit(&client, sub).await;
    let view = wait_running(&client, spread, 2).await;
    let mut zones: Vec<String> = view
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| matches!(t.state, TaskState::Running { .. }))
        .map(|(i, _)| node_of(&cluster, &view, i))
        .collect();
    zones.sort();
    assert_eq!(zones, vec!["a", "b"]);
    assert!(placement(&client, spread).await.waiting.is_some());

    // A database pinned to a; its cache goes next to it, a batch job
    // away from it.
    let mut db = submission(vec![vec!["db", "slow"]]);
    db.name = "db".into();
    db.selector = "zone=x".parse().unwrap();
    let db = submit(&client, db).await;
    wait_running(&client, db, 1).await;
    let mut near = submission(vec![vec!["cache", "slow"]]);
    near.placement.near = vec!["db".into()];
    let near = submit(&client, near).await;
    let mut away = submission(vec![vec!["batch", "slow"]]);
    away.placement.away = vec!["db".into()];
    let away = submit(&client, away).await;
    let v = wait_running(&client, near, 1).await;
    assert_eq!(node_of(&cluster, &v, 0), "a");
    let v = wait_running(&client, away, 1).await;
    assert_eq!(node_of(&cluster, &v, 0), "b");
    for id in [spread, db, near, away] {
        wait_job(&client, id, 60).await;
    }

    // One CPU at a time for the owner: two WASM tasks run one by one.
    client
        .call(AdminOp::SetResources {
            account: "owner".into(),
            quota: Box::new(ResourceQuota {
                cpus: Some(1),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    let id = submit(
        &client,
        submission(vec![vec!["0", "slow"], vec!["1", "slow"]]),
    )
    .await;
    wait_running(&client, id, 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let view = job(&client, id).await;
    assert_eq!(
        view.tasks
            .iter()
            .filter(|t| matches!(t.state, TaskState::Running { .. }))
            .count(),
        1,
        "{view:#?}"
    );
    let AdminResponse::Resources(r) = client.call(AdminOp::Resources).await.unwrap() else {
        panic!()
    };
    let owner = r.iter().find(|r| r.account == "owner").unwrap();
    assert_eq!((owner.used.cpus, owner.quota.cpus), (1, Some(1)));
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Succeeded);

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedules_run_in_their_time_zone() {
    init_tracing();
    let dir = tempdir("c");
    let cluster = start_cluster(dir.clone()).await.unwrap();
    let client = admin(&cluster).await;
    let spec = ScheduleSpec {
        name: "morning".into(),
        cron: "0 9 * * *".into(),
        job: submission(vec![vec!["hi"]]),
        overlap: false,
        tz: Some("Asia/Seoul".into()),
    };
    client
        .call(AdminOp::set_schedule(spec.clone()))
        .await
        .unwrap();
    let AdminResponse::Schedules(list) = client.call(AdminOp::Schedules).await.unwrap() else {
        panic!()
    };
    let s = list.iter().find(|s| s.name == "morning").unwrap();
    assert_eq!(s.cron, "0 9 * * * (Asia/Seoul)");
    // 09:00 in Seoul is 00:00 UTC.
    let next = s.next_unix.unwrap();
    assert_eq!(next % 86_400, 0, "next run at {next}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(next > now && next <= now + 86_400);

    // Kept across a restart of the control plane.
    client.close().await;
    cluster.shutdown().await.unwrap();
    let cluster = start_cluster(dir).await.unwrap();
    let client = admin(&cluster).await;
    let AdminResponse::Schedules(list) = client.call(AdminOp::Schedules).await.unwrap() else {
        panic!()
    };
    assert_eq!(list[0].cron, "0 9 * * * (Asia/Seoul)");
    assert_eq!(list[0].next_unix, Some(next));
    client.close().await;
    cluster.shutdown().await.unwrap();
}

/// True when a Docker daemon that runs Linux containers is reachable.
fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["version", "--format", "{{.Server.Os}}"])
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "linux")
        .unwrap_or(false)
}

/// A shell loop that saves its count on SIGTERM and goes on from it.
const COUNTER_SH: &str = r#"n=$(cat /checkpoint/n 2>/dev/null || echo 0); start=$n
trap 'echo $n > /checkpoint/n; exit 0' TERM
while [ $n -lt 40 ]; do sleep 0.1 & wait $!; n=$((n+1)); done
echo "start=$start"
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docker_checkpoint_survives_a_drain() {
    if !docker_available() {
        eprintln!("skipping: docker is not available");
        return;
    }
    init_tracing();
    // Pulled first, so the count is going when the drain comes.
    let pulled = std::process::Command::new("docker")
        .args(["pull", "-q", "busybox:1.36"])
        .status()
        .is_ok_and(|s| s.success());
    if !pulled {
        eprintln!("skipping: could not pull busybox");
        return;
    }
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent_with("da", 1, ticket.clone(), Some("docker".into())).await;
    let b = spawn_agent_with("db", 1, ticket, Some("docker".into())).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    edit(&client, "db", cordon()).await;

    let mut sub = submission(vec![vec![]]);
    sub.name = "docker-counter".into();
    sub.executor = Executor::Docker;
    sub.module = vec![];
    sub.docker = Some(DockerSpec {
        image: "busybox:1.36".into(),
        command: vec!["sh".into(), "-c".into(), COUNTER_SH.into()],
        network: false,
        gpus: None,
    });
    sub.limits = Limits {
        timeout_ms: 120_000,
        memory_bytes: 64 << 20,
        ..Default::default()
    };
    sub.placement.checkpoint = Some(Checkpoint { grace_ms: 10_000 });
    let id = submit(&client, sub).await;
    wait_running(&client, id, 1).await;
    // Past the image pull: the count is going.
    tokio::time::sleep(Duration::from_secs(3)).await;
    edit(&client, "db", uncordon()).await;
    client
        .call(AdminOp::Drain { node: "da".into() })
        .await
        .unwrap();
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let out =
        String::from_utf8_lossy(view.tasks[0].stdout.as_deref().unwrap_or_default()).to_string();
    let start: u32 = out
        .trim()
        .strip_prefix("start=")
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unexpected output {out:?}"));
    assert!(start > 0, "it went on from its checkpoint: {out:?}");
    assert_eq!(node_of(&cluster, &view, 0), "db");
    assert_eq!(placement(&client, id).await.checkpoints[0].resumed, 1);

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}
