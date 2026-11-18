//! End-to-end tests of 1.21 over real iroh connections: a task that fails
//! twice succeeds with two retries, a rerun runs only the failed tasks, a
//! run's peak memory is what it allocated, and (with Docker) a task that
//! checkpoints every 10 s goes on from its checkpoint when its node dies.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    attempt::{EndKind, JobRun, JobRunsView, RerunWhich, RetryPolicy},
    job::{DockerSpec, Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{AdminOp, AdminResponse, JobState, JobView, TaskState},
    net::NetMode,
    sched::{Checkpoint, PlacementView},
    ticket::JoinTicket,
};
use tokio::task::JoinHandle;

/// By its first argument: `ok` prints "ok"; `fail` exits 1; `retry` exits
/// 1 until `PUMASI_RETRY` is 2, then prints "ok"; `mem` grows its memory
/// by 64 MiB, touches every page of it and prints "ok".
const RUNS_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "environ_sizes_get" (func $esizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "environ_get" (func $environ_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 1)
  (data (i32.const 16) "PUMASI_RETRY=")
  (data (i32.const 32) "ok\n")
  (func $ok
    (i32.store (i32.const 40) (i32.const 32))
    (i32.store (i32.const 44) (i32.const 3))
    (drop (call $fd_write (i32.const 1) (i32.const 40) (i32.const 1) (i32.const 48))))
  (func $is_retry (param $p i32) (result i32) (local $i i32)
    (loop $l
      (if (i32.ne (i32.load8_u (i32.add (local.get $p) (local.get $i)))
                  (i32.load8_u (i32.add (i32.const 16) (local.get $i))))
        (then (return (i32.const 0))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (i32.const 13))))
    (i32.const 1))
  (func $retry (result i32) (local $n i32) (local $i i32) (local $p i32)
    (drop (call $esizes (i32.const 0) (i32.const 4)))
    (drop (call $environ_get (i32.const 4096) (i32.const 8192)))
    (local.set $n (i32.load (i32.const 0)))
    (block $done (loop $l
      (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
      (local.set $p (i32.load (i32.add (i32.const 4096) (i32.mul (local.get $i) (i32.const 4)))))
      (if (call $is_retry (local.get $p))
        (then (return (i32.sub (i32.load8_u (i32.add (local.get $p) (i32.const 13))) (i32.const 48)))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br $l)))
    (i32.const -1))
  (func (export "_start") (local $c i32) (local $i i32)
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $args_get (i32.const 64) (i32.const 1024)))
    (local.set $c (i32.load8_u (i32.load (i32.const 68))))
    (if (i32.eq (local.get $c) (i32.const 102)) (then (call $exit (i32.const 1))))
    (if (i32.eq (local.get $c) (i32.const 114))
      (then (if (i32.lt_s (call $retry) (i32.const 2)) (then (call $exit (i32.const 1))))))
    (if (i32.eq (local.get $c) (i32.const 109))
      (then
        (drop (memory.grow (i32.const 1024)))
        (local.set $i (i32.const 65536))
        (block $d (loop $l
          (br_if $d (i32.ge_u (local.get $i) (i32.const 67174400)))
          (i32.store8 (local.get $i) (i32.const 1))
          (local.set $i (i32.add (local.get $i) (i32.const 4096)))
          (br $l)))))
    (call $ok))
)
"#;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-runs-{tag}-{}", rand_u64()));
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
        gpu_mps: false,
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

/// One WASM task per argument.
fn submission(args: &[&str]) -> JobSubmission {
    JobSubmission {
        placement: Default::default(),
        gpu_ext: Default::default(),
        net: Default::default(),
        name: "runs".into(),
        executor: Executor::Wasm,
        module: wat::parse_str(RUNS_WAT).unwrap(),
        docker: None,
        files: vec![],
        tasks: args
            .iter()
            .map(|a| TaskInput {
                args: vec![a.to_string()],
                ..Default::default()
            })
            .collect(),
        limits: Limits {
            timeout_ms: 60_000,
            memory_bytes: 256 << 20,
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
        run: Default::default(),
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

async fn placement(client: &AdminClient, id: JobId) -> PlacementView {
    let AdminResponse::Placement(p) = client.call(AdminOp::Placement { id }).await.unwrap() else {
        panic!("expected a placement view");
    };
    *p
}

async fn runs(client: &AdminClient, id: JobId) -> JobRunsView {
    let AdminResponse::JobRuns(r) = client.call(AdminOp::JobRuns { id }).await.unwrap() else {
        panic!("expected the job's runs");
    };
    *r
}

fn stdout(view: &JobView, index: usize) -> String {
    String::from_utf8_lossy(view.tasks[index].stdout.as_deref().unwrap_or_default()).to_string()
}

/// The gate: a task that fails twice succeeds with retries = 2, its
/// retries on the other node, and each run says what it used.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_failing_twice_succeeds_with_two_retries() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("ra", 1, ticket.clone()).await;
    let b = spawn_agent("rb", 1, ticket).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    let mut sub = submission(&["retry"]);
    sub.run = JobRun {
        retry: Some(RetryPolicy {
            retries: 2,
            on: vec![],
            delay_ms: 200,
        }),
        checkpoint_every_ms: None,
    };
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 120).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(stdout(&view, 0), "ok\n");
    let r = runs(&client, id).await;
    let t = &r.tasks[0];
    assert_eq!(t.retries, 2, "{r:#?}");
    assert_eq!(t.attempts.len(), 3, "{r:#?}");
    assert_eq!(t.attempts[0].end, Some(EndKind::Exit(1)));
    assert_eq!(t.attempts[1].end, Some(EndKind::Exit(1)));
    assert_eq!(t.attempts[2].end, Some(EndKind::Succeeded));
    assert!(t.attempts[0].retried && t.attempts[1].retried);
    assert_ne!(
        t.attempts[0].node, t.attempts[1].node,
        "the first retry went to the other node"
    );
    // What the WASM runs used: their memory and CPU.
    let u = t.attempts[2].usage.expect("usage of the last run");
    assert!(u.peak_memory_bytes.is_some_and(|m| m >= 64 << 10), "{u:?}");
    assert!(u.cpu_ms.is_some(), "{u:?}");
    assert!(r.total.peak_memory_bytes.is_some());
    // The retries are events.
    let AdminResponse::Events(events) = client
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 200,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(events.iter().filter(|e| e.kind == "task.retry").count(), 2);

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

/// The gate: a rerun runs only the failed tasks, and keeps the others'
/// results.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rerun_runs_only_the_failed_tasks() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("rr", 3, ticket).await;
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;
    let id = submit(&client, submission(&["ok", "fail", "ok"])).await;
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Failed, "{view:#?}");
    let AdminResponse::JobRuns(r) = client
        .call(AdminOp::Rerun {
            id,
            which: RerunWhich::Failed,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.tasks.len(), 3);
    // (The task fails again at once, so the job may have ended already.)
    let view = job(&client, id).await;
    assert!(matches!(view.tasks[0].state, TaskState::Succeeded));
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Failed, "it fails again");
    let r = runs(&client, id).await;
    assert_eq!(r.tasks[0].attempts.len(), 1, "{r:#?}");
    assert_eq!(r.tasks[2].attempts.len(), 1, "{r:#?}");
    assert_eq!(r.tasks[1].attempts.len(), 2, "{r:#?}");
    assert!(r.tasks[1].attempts[1].rerun);
    assert_eq!(stdout(&view, 0), "ok\n", "the others keep their output");
    assert_eq!(stdout(&view, 2), "ok\n");
    // A running job is not rerun.
    let id2 = submit(&client, submission(&["ok"])).await;
    let _ = wait_job(&client, id2, 60).await;
    assert!(
        client
            .call(AdminOp::Rerun {
                id: id2,
                which: RerunWhich::Tasks(vec![5]),
            })
            .await
            .is_err(),
        "no task 5"
    );

    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();
}

/// The gate, for WASM: the peak memory of a run is what it allocated
/// (64 MiB beside its first page), within 10%.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wasm_peak_memory_is_what_the_task_allocated() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("rm", 1, ticket).await;
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;
    let mut sub = submission(&["mem"]);
    sub.limits.memory_bytes = 512 << 20;
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let r = runs(&client, id).await;
    let peak = r.tasks[0].attempts[0]
        .usage
        .and_then(|u| u.peak_memory_bytes)
        .expect("a peak");
    let want = 1025u64 * 65536;
    assert!(
        peak.abs_diff(want) * 10 <= want,
        "peak {peak}, allocated {want}"
    );
    // It asked for 512 MiB and used about 64: a hint says so.
    assert!(
        r.tasks[0].overasked.iter().any(|h| h.contains("memory")),
        "{r:#?}"
    );

    client.close().await;
    a.kill().await;
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

fn pull_busybox() -> bool {
    std::process::Command::new("docker")
        .args(["pull", "-q", "busybox:1.36"])
        .status()
        .is_ok_and(|s| s.success())
}

fn docker_job(script: &str) -> JobSubmission {
    let mut sub = submission(&[""]);
    sub.tasks = vec![TaskInput::default()];
    sub.executor = Executor::Docker;
    sub.module = vec![];
    sub.docker = Some(DockerSpec {
        image: "busybox:1.36".into(),
        command: vec!["sh".into(), "-c".into(), script.into()],
        network: false,
        gpus: None,
    });
    sub.limits = Limits {
        timeout_ms: 180_000,
        memory_bytes: 512 << 20,
        ..Default::default()
    };
    sub
}

/// The gate, for Docker: the peak memory of a run that fills a 256 MiB
/// buffer is within 10% of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docker_peak_memory_is_what_the_task_allocated() {
    if !docker_available() || !pull_busybox() {
        eprintln!("skipping: docker (or busybox) is not available");
        return;
    }
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent_with("dm", 1, ticket, Some("docker".into())).await;
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;
    // `dd` reads into one 256 MiB buffer, touching all of it.
    let sub =
        docker_job("dd if=/dev/zero of=/dev/null bs=256M count=1 2>/dev/null; sleep 2; echo done");
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let r = runs(&client, id).await;
    let u = r.tasks[0].attempts[0].usage.expect("usage");
    let peak = u.peak_memory_bytes.expect("a peak");
    let want = 256u64 << 20;
    assert!(
        peak.abs_diff(want) * 10 <= want,
        "peak {peak}, allocated {want}: {u:?}"
    );
    assert!(u.cpu_ms.is_some(), "{u:?}");

    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();
}

/// Counts to 60, a step a second, and saves the count to /checkpoint/n
/// only when asked (the request file); starts from what it finds there.
const PERIODIC_SH: &str = r#"n=$(cat /checkpoint/n 2>/dev/null || echo 0); start=$n
while [ $n -lt 60 ]; do
  sleep 1; n=$((n+1))
  if [ -f /checkpoint/.pumasi-checkpoint ]; then echo $n > /checkpoint/n; rm /checkpoint/.pumasi-checkpoint; fi
done
echo "start=$start"
"#;

/// The gate: a Docker task that checkpoints every 10 s goes on elsewhere
/// from its last checkpoint when the agent running it dies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docker_task_resumes_from_its_periodic_checkpoint_when_its_agent_dies() {
    if !docker_available() || !pull_busybox() {
        eprintln!("skipping: docker (or busybox) is not available");
        return;
    }
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent_with("pa", 1, ticket.clone(), Some("docker".into())).await;
    let b = spawn_agent_with("pb", 1, ticket, Some("docker".into())).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    let mut sub = docker_job(PERIODIC_SH);
    sub.name = "periodic".into();
    sub.max_attempts = 3;
    sub.placement.checkpoint = Some(Checkpoint { grace_ms: 10_000 });
    sub.run.checkpoint_every_ms = Some(10_000);
    let id = submit(&client, sub).await;
    let view = wait_running(&client, id, 1).await;
    let first = view.tasks[0].node.clone().unwrap();
    // The first periodic checkpoint.
    let mut saved = false;
    for _ in 0..300 {
        if !placement(&client, id).await.checkpoints.is_empty() {
            saved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        saved,
        "no periodic checkpoint: {:#?}",
        placement(&client, id).await
    );
    let names = cluster.status().nodes;
    let dying = names
        .iter()
        .find(|n| n.id == first)
        .unwrap()
        .info
        .name
        .clone();
    let (dead, alive) = if dying == "pa" { (a, b) } else { (b, a) };
    dead.kill().await;
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let out = stdout(&view, 0);
    let start: u32 = out
        .trim()
        .strip_prefix("start=")
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unexpected output {out:?}"));
    assert!(start >= 10, "it went on from its checkpoint: {out:?}");
    assert_ne!(view.tasks[0].node.as_deref(), Some(first.as_str()));
    let p = placement(&client, id).await;
    assert_eq!(p.checkpoints[0].resumed, 1, "{p:#?}");
    let r = runs(&client, id).await;
    assert_eq!(r.tasks[0].attempts[0].end, Some(EndKind::Lost), "{r:#?}");

    client.close().await;
    alive.kill().await;
    cluster.shutdown().await.unwrap();
}
