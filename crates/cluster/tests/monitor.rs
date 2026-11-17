//! End-to-end tests of monitoring (phase 11) over real iroh connections:
//! node metrics, the Prometheus endpoint, live task output, events, and a
//! node going offline reaching a webhook; and (1.6) probes between
//! agents, silences, and metrics kept over a restart.

use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    job::{Executor, JobSubmission, Limits, TaskInput, Tier},
    monitor::{AlertPolicy, LogChunk, SilenceSpec, WebhookFormat, WebhookSpec, lint_exposition},
    msg::{AdminOp, AdminResponse, JobState, TaskState},
    net::NetMode,
    ticket::JoinTicket,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

/// Writes the wall clock (8 bytes, nanoseconds, little endian) to stdout,
/// busy-waits 3 seconds, then writes "done\n".
const CLOCK_THEN_DONE_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "clock_time_get" (func $now (param i32 i64 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 64) "done\n")
  (func $mono (result i64)
    (drop (call $now (i32.const 1) (i64.const 1000) (i32.const 48)))
    (i64.load (i32.const 48)))
  (func (export "_start")
    (local $start i64)
    (drop (call $now (i32.const 0) (i64.const 1000) (i32.const 32)))
    (i32.store (i32.const 8) (i32.const 32))
    (i32.store (i32.const 12) (i32.const 8))
    (drop (call $fd_write (i32.const 1) (i32.const 8) (i32.const 1) (i32.const 16)))
    (local.set $start (call $mono))
    (block $done (loop $spin
      (br_if $done (i64.ge_u (i64.sub (call $mono) (local.get $start)) (i64.const 3000000000)))
      (br $spin)))
    (i32.store (i32.const 8) (i32.const 64))
    (i32.store (i32.const 12) (i32.const 5))
    (drop (call $fd_write (i32.const 1) (i32.const 8) (i32.const 1) (i32.const 16)))))
"#;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-monitor-{tag}-{}", rand_u64()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(SystemTime::now())
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

async fn start_cluster(dir: PathBuf, metrics_token: Option<&str>) -> Result<Cluster> {
    start_cluster_probing(dir, metrics_token, None).await
}

async fn start_cluster_probing(
    dir: PathBuf,
    metrics_token: Option<&str>,
    probe_every: Option<Duration>,
) -> Result<Cluster> {
    let mut config = ClusterConfig::new(dir);
    config.name = "test-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    config.metrics_addr = Some("127.0.0.1:0".parse().unwrap());
    config.metrics_token = metrics_token.map(String::from);
    config.probe_every = probe_every;
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

async fn spawn_agent(name: &str, ticket: JoinTicket) -> RunningAgent {
    let config = AgentConfig {
        data_dir: tempdir(name),
        name: name.into(),
        slots: Some(2),
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

/// A plain HTTP/1.1 GET; returns the status line and the body.
async fn http_get(addr: SocketAddr, path: &str, token: Option<&str>) -> (String, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n{auth}\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8(buf).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    (head.lines().next().unwrap().to_string(), body.to_string())
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_live_logs_events_and_prometheus() {
    init_tracing();
    let token = "scrape-secret";
    let cluster = start_cluster(tempdir("cp"), Some(token)).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let agent = spawn_agent("mon-a", ticket).await;
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;

    // The agent's first sample comes right after it joins.
    let mut metrics = None;
    for _ in 0..100 {
        let AdminResponse::Metrics(m) = client
            .call(AdminOp::Metrics {
                node: Some("mon-a".into()),
                since_secs: 3600,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        if m.nodes[0].latest.is_some() && !m.nodes[0].series.is_empty() {
            metrics = Some(m);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let m = metrics.expect("no metrics from the agent");
    let n = &m.nodes[0];
    let latest = n.latest.as_ref().unwrap();
    assert!(latest.memory_total > 0, "{latest:?}");
    assert!(latest.disk_total > 0, "{latest:?}");
    assert_eq!(n.step_secs, 10);

    // A task that prints the time, works 3 s, then prints "done": its
    // first output shows while it runs, within a second of being printed.
    let id = {
        let sub = JobSubmission {
            placement: Default::default(),
            gpu_ext: Default::default(),
            name: "clock".into(),
            executor: Executor::Wasm,
            module: wat::parse_str(CLOCK_THEN_DONE_WAT).unwrap(),
            docker: None,
            files: vec![],
            tasks: vec![TaskInput::default()],
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
        };
        let AdminResponse::Submitted { id } =
            client.call(AdminOp::Submit(Box::new(sub))).await.unwrap()
        else {
            panic!()
        };
        id
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut out = Vec::new();
    let mut attempt = 0;
    let mut latency = None;
    loop {
        assert!(Instant::now() < deadline, "no output; got {out:?}");
        let AdminResponse::Logs(c) = client
            .call(AdminOp::Logs {
                job: id,
                index: 0,
                attempt,
                stdout_from: out.len() as u64,
                stderr_from: 0,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        let c: LogChunk = *c;
        attempt = c.attempt;
        out.extend_from_slice(&c.stdout);
        if latency.is_none() && out.len() >= 8 {
            let printed = u64::from_le_bytes(out[..8].try_into().unwrap()) as u128;
            let seen = unix_nanos();
            assert!(
                matches!(c.state, TaskState::Running { .. }),
                "output only came at the end: {:?}",
                c.state
            );
            assert!(c.live);
            latency = Some(Duration::from_nanos(seen.saturating_sub(printed) as u64));
        }
        if c.done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let latency = latency.expect("the clock bytes");
    eprintln!("live output latency: {latency:?}");
    assert!(
        latency < Duration::from_secs(1),
        "output took {latency:?} to show"
    );
    assert_eq!(&out[8..], b"done\n");

    // The job finished: events say so, and the counters count it.
    let mut kinds = Vec::new();
    for _ in 0..50 {
        let AdminResponse::Events(e) = client
            .call(AdminOp::Events {
                after: 0,
                since_unix: 0,
                limit: 100,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        kinds = e.iter().map(|e| e.kind.clone()).collect();
        if kinds.iter().any(|k| k == "job.succeeded") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for k in [
        "node.joined",
        "node.online",
        "job.submitted",
        "job.succeeded",
    ] {
        assert!(kinds.iter().any(|x| x == k), "no {k} in {kinds:?}");
    }
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
    assert_eq!(view.summary.state, JobState::Succeeded);

    // Prometheus: a bearer token is needed, and the text scrapes.
    let addr = cluster.metrics_addr().unwrap();
    let (status, _) = http_get(addr, "/metrics", None).await;
    assert!(status.contains("401"), "{status}");
    let (status, _) = http_get(addr, "/nope", Some(token)).await;
    assert!(status.contains("404"), "{status}");
    let (status, body) = http_get(addr, "/metrics", Some(token)).await;
    assert!(status.contains("200"), "{status}");
    let samples = lint_exposition(&body).unwrap_or_else(|e| panic!("{e:#}\n{body}"));
    assert!(samples > 20, "{body}");
    assert!(body.contains("pumasi_node_up{node=\"mon-a\""), "{body}");
    assert!(body.contains("pumasi_node_memory_total_bytes{"), "{body}");
    assert!(
        body.contains("pumasi_runs_total{outcome=\"succeeded\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("pumasi_jobs_finished_total{state=\"succeeded\"} 1"),
        "{body}"
    );
    // CI checks the scrape with Prometheus' own promtool when it has one.
    if let Ok(promtool) = std::env::var("PUMASI_PROMTOOL") {
        let mut child = std::process::Command::new(promtool)
            .args(["check", "metrics"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(body.as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        let report = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        eprintln!("promtool: {report}");
        // promtool also gives lint advice (exit 3 for "problems" in some
        // versions); errors in the format itself exit 1.
        assert_ne!(out.status.code(), Some(1), "{report}");
    }

    client.close().await;
    agent.kill().await;
    cluster.shutdown().await.unwrap();
}

/// A webhook receiver: answers 200 and passes each request body on.
async fn receiver() -> (SocketAddr, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = s.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len = head
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        if body.len() >= len {
                            let _ = tx.send(body[..len].to_string());
                            let _ = s
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                                .await;
                            return;
                        }
                    }
                }
            });
        }
    });
    (addr, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_going_offline_reaches_the_webhook() {
    init_tracing();
    let (hook, mut bodies) = receiver().await;
    let cluster = start_cluster(tempdir("cp-hook"), None).await.unwrap();
    let client = admin(&cluster).await;
    client
        .call(AdminOp::SetAlertPolicy(Box::new(AlertPolicy {
            node_offline_secs: Some(1),
            // The runner's own disk may be nearly full.
            disk_percent: None,
            temp_c: None,
            ..Default::default()
        })))
        .await
        .unwrap();
    client
        .call(AdminOp::SetWebhook(Box::new(WebhookSpec {
            name: "ops".into(),
            url: format!("http://{hook}/alerts"),
            format: WebhookFormat::Json,
        })))
        .await
        .unwrap();
    client
        .call(AdminOp::TestWebhook { name: "ops".into() })
        .await
        .unwrap();
    let test: serde_json::Value = serde_json::from_str(&bodies.recv().await.unwrap()).unwrap();
    assert_eq!(test["rule"], "test");

    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let agent = spawn_agent("mon-b", ticket).await;
    wait_online(&cluster, 1).await;
    let off = Instant::now();
    agent.kill().await;
    let alert = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let b = bodies.recv().await.unwrap();
            let v: serde_json::Value = serde_json::from_str(&b).unwrap();
            if v["rule"] == "node_offline" {
                return v;
            }
        }
    })
    .await
    .expect("no webhook alert");
    let took = off.elapsed();
    eprintln!("node offline alert after {took:?}");
    assert_eq!(alert["status"], "firing");
    assert_eq!(alert["subject"], "mon-b");
    assert_eq!(alert["cluster"], "test-cluster");
    // Missed heartbeats for 2 s, then 1 s offline.
    assert!(took < Duration::from_secs(10), "took {took:?}");

    // The receiver has the body before the sender records the reply.
    let deadline = Instant::now() + Duration::from_secs(5);
    let v = loop {
        let AdminResponse::Alerts(v) = client.call(AdminOp::Alerts).await.unwrap() else {
            panic!()
        };
        if v.webhooks[0].sent == 2 || Instant::now() > deadline {
            break v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(v.firing.len(), 1, "{:?}", v.firing);
    assert_eq!(v.webhooks[0].sent, 2, "the test and the alert");
    assert!(v.webhooks[0].target.ends_with("/…"), "URL path hidden");

    client.close().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probes_silences_and_metrics_over_a_restart() {
    init_tracing();
    let dir = tempdir("cp-net");
    let cluster = start_cluster_probing(dir.clone(), None, Some(Duration::from_millis(300)))
        .await
        .unwrap();
    let ticket = || {
        cluster
            .create_join_ticket(Duration::from_secs(60), None)
            .unwrap()
    };
    let a = spawn_agent("net-a", ticket()).await;
    let b = spawn_agent("net-b", ticket()).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;

    // Both directions get measured, directly on the loopback.
    let deadline = Instant::now() + Duration::from_secs(30);
    let net = loop {
        let AdminResponse::Net(v) = client.call(AdminOp::Net).await.unwrap() else {
            panic!()
        };
        let ok = |f, t| {
            v.link(f, t)
                .is_some_and(|l| l.error.is_none() && l.bps.is_some())
        };
        if ok("net-a", "net-b") && ok("net-b", "net-a") {
            break v;
        }
        assert!(Instant::now() < deadline, "no probes: {v:#?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let l = net.link("net-a", "net-b").unwrap();
    eprintln!("probe a→b: {l:?}");
    assert!(l.rtt_us.is_some());
    assert_eq!(l.direct, Some(true));
    assert_eq!(net.nodes, ["net-a", "net-b"]);

    // A silence keeps b's offline alert out of the notices.
    client
        .call(AdminOp::SetAlertPolicy(Box::new(AlertPolicy {
            node_offline_secs: Some(0),
            disk_percent: None,
            temp_c: None,
            ..Default::default()
        })))
        .await
        .unwrap();
    let AdminResponse::AlertTuning(v) = client
        .call(AdminOp::Silence(Box::new(SilenceSpec {
            rule: Some("node_offline".into()),
            nodes: Some("name=net-b".parse().unwrap()),
            secs: 3600,
            comment: "maintenance".into(),
        })))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(v.silences.len(), 1);
    b.kill().await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let AdminResponse::AlertTuning(v) = client.call(AdminOp::AlertTuning).await.unwrap() else {
            panic!()
        };
        if v.silenced == [("node_offline".to_string(), "net-b".to_string())] {
            break;
        }
        assert!(Instant::now() < deadline, "b not silenced: {v:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The first samples, at 10 s steps, are still there after a restart.
    let samples = |c: AdminClient| async move {
        let AdminResponse::Metrics(m) = c
            .call(AdminOp::Metrics {
                node: Some("net-a".into()),
                since_secs: 3600,
            })
            .await
            .unwrap()
        else {
            panic!()
        };
        c.close().await;
        m.nodes[0].series.len()
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let before = loop {
        let n = samples(admin(&cluster).await).await;
        if n > 0 {
            break n;
        }
        assert!(Instant::now() < deadline, "no samples");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();

    let cluster = start_cluster(dir, None).await.unwrap();
    let client = admin(&cluster).await;
    let after = samples(admin(&cluster).await).await;
    assert!(
        after >= before,
        "{after} samples after the restart, {before} before"
    );
    let AdminResponse::Net(v) = client.call(AdminOp::Net).await.unwrap() else {
        panic!()
    };
    assert!(v.link("net-a", "net-b").is_some(), "links kept: {v:?}");
    let AdminResponse::AlertTuning(v) = client.call(AdminOp::AlertTuning).await.unwrap() else {
        panic!()
    };
    assert_eq!(v.silences.len(), 1, "silences kept");
    client.close().await;
    cluster.shutdown().await.unwrap();
}

/// Release 1.15: per-node thresholds and silences from a `cluster.toml`
/// (with the diff `apply` shows), events and alerts by message key,
/// metrics pushed when a sample arrives, and agents probing the control
/// plane's store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_thresholds_silences_keyed_events_and_pushed_metrics() {
    use pumasi_proto::{
        manage::ClusterFile,
        monitor::{STORE_LINK, file_silence_by, file_silence_id},
    };
    init_tracing();
    let cluster = start_cluster_probing(tempdir("cp-31"), None, Some(Duration::from_millis(300)))
        .await
        .unwrap();
    let ticket = || {
        cluster
            .create_join_ticket(Duration::from_secs(60), None)
            .unwrap()
    };
    let a = spawn_agent("p31-a", ticket()).await;
    let b = spawn_agent("p31-b", ticket()).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;

    // Agents measure the store too: the matrix gets a column for it.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let AdminResponse::Net(v) = client.call(AdminOp::Net).await.unwrap() else {
            panic!()
        };
        if let Some(l) = v.link("p31-a", STORE_LINK)
            && l.error.is_none()
            && l.bps.is_some()
            && l.rtt_us.is_some()
        {
            eprintln!("probe a→store: {l:?}");
            break;
        }
        assert!(Instant::now() < deadline, "no store probe: {v:#?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A pushed sample: answered when the next one arrives, not on a timer.
    let push = |after: u64, wait: u32| {
        let c = &client;
        async move {
            let AdminResponse::MetricsPush(p) = c
                .call(AdminOp::MetricsWait {
                    node: Some("p31-a".into()),
                    since_secs: 600,
                    after_unix: after,
                    wait_secs: wait,
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            p
        }
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    let first = loop {
        let p = push(0, 0).await;
        let latest = p.view.nodes.first().map_or(0, |n| n.latest_unix);
        if latest > 0 {
            break latest;
        }
        assert!(Instant::now() < deadline, "no samples");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let started = Instant::now();
    let p = push(first, 25).await;
    let id = p.view.nodes[0].id.clone();
    assert!(
        p.view.nodes[0].latest_unix > first,
        "the wait ended without a new sample"
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{:?}",
        started.elapsed()
    );
    // What came beyond the metrics (CPU package power where it can be
    // read): every node that sent a sample has an entry.
    assert!(p.extra_of(&id).is_some(), "{:?}", p.extra);

    // Thresholds for one node, from the file, with the diff first.
    let until = pumasi_proto::audit::rfc3339(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600,
    );
    let file = |silence: bool| {
        let mut text = String::from(
            "[alerts]\nnode_offline = \"1s\"\ndisk = \"off\"\ntemperature = \"off\"\nrepeat = \"1h\"\n\n\
             [alerts.nodes.p31-a]\nnode_offline = \"off\"\n\n\
             [alerts.labels.\"os=plan9\"]\ndisk = \"99%\"\n",
        );
        if silence {
            text.push_str(&format!(
                "\n[silences.quiet-b]\nrule = \"node_offline\"\nnodes = \"name=p31-b\"\nuntil = \"{until}\"\ncomment = \"moving it\"\n"
            ));
        }
        ClusterFile::parse(&text)
            .unwrap()
            .resolve(std::path::Path::new("."), 0)
            .unwrap()
    };
    let apply = |mut spec: pumasi_proto::manage::ApplySpec, dry: bool, prune: bool| {
        let c = &client;
        spec.dry_run = dry;
        spec.prune = prune;
        async move {
            let AdminResponse::Applied(changes) = c.call(AdminOp::apply(spec)).await.unwrap()
            else {
                panic!()
            };
            changes
        }
    };
    let preview = apply(file(false), true, false).await;
    eprintln!("preview: {preview:#?}");
    for want in [
        "alerts: policy updated",
        "alerts: repeat once -> every 1h",
        "alerts on name=p31-a: node_offline off",
        "alerts on os=plan9: disk 99%",
    ] {
        assert!(preview.iter().any(|c| c == want), "{want}: {preview:#?}");
    }
    let AdminResponse::AlertTuning(v) = client.call(AdminOp::AlertTuning).await.unwrap() else {
        panic!()
    };
    assert!(v.tuning.overrides.is_empty(), "a preview changes nothing");
    assert_eq!(apply(file(false), false, false).await, preview);
    assert!(
        apply(file(false), false, false).await.is_empty(),
        "applied again: nothing"
    );
    let AdminResponse::AlertTuning(v) = client.call(AdminOp::AlertTuning).await.unwrap() else {
        panic!()
    };
    assert_eq!(v.tuning.repeat_secs, Some(3600));
    assert_eq!(v.tuning.overrides.len(), 2);

    // Both go offline: only b's alert fires, a's threshold is off.
    a.kill().await;
    b.kill().await;
    let deadline = Instant::now() + Duration::from_secs(20);
    let alerts = loop {
        let AdminResponse::Alerts(al) = client.call(AdminOp::Alerts).await.unwrap() else {
            panic!()
        };
        if al.firing.iter().any(|f| f.rule == "node_offline") {
            break al;
        }
        assert!(Instant::now() < deadline, "no offline alert: {al:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let subjects: Vec<&str> = alerts
        .firing
        .iter()
        .filter(|f| f.rule == "node_offline")
        .map(|f| f.subject.as_str())
        .collect();
    assert_eq!(subjects, ["p31-b"], "{alerts:?}");

    // The alert and the events by key, for a Korean dashboard.
    let AdminResponse::AlertTexts(texts) = client.call(AdminOp::AlertTexts).await.unwrap() else {
        panic!()
    };
    let t = texts
        .iter()
        .find(|t| t.rule == "node_offline" && t.subject == "p31-b")
        .unwrap_or_else(|| panic!("{texts:?}"));
    let ko = t.text.render(true).unwrap();
    assert!(ko.contains("p31-b") && ko.contains("오프라인"), "{ko}");
    let AdminResponse::EventsKeyed(events) = client
        .call(AdminOp::EventsKeyed {
            after: 0,
            since_unix: 0,
            limit: 100,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    let offline = events
        .iter()
        .find(|e| e.event.kind == "node.offline" && e.event.node.as_deref() == Some("p31-b"))
        .unwrap_or_else(|| panic!("{events:#?}"));
    let text = offline.text.as_ref().expect("a key");
    assert_eq!(
        text.render(false).as_deref(),
        Some(offline.event.message.as_str())
    );
    assert!(text.render(true).unwrap().contains("오프라인"));

    // A silence from the file keeps b quiet; applying again finds it.
    let changes = apply(file(true), false, false).await;
    assert!(
        changes.len() == 1 && changes[0].starts_with("silence quiet-b: created, until "),
        "{changes:?}"
    );
    assert!(apply(file(true), false, false).await.is_empty());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let AdminResponse::AlertTuning(v) = client.call(AdminOp::AlertTuning).await.unwrap() else {
            panic!()
        };
        let s = v
            .silences
            .iter()
            .find(|s| s.id == file_silence_id("quiet-b"))
            .unwrap_or_else(|| panic!("{v:?}"));
        // The name rides along (1.17), so the editor can write it back.
        assert_eq!(s.by, file_silence_by("quiet-b"));
        assert_eq!(s.file_name(), Some("quiet-b"));
        assert_eq!(s.comment, "moving it");
        if v.silenced == [("node_offline".to_string(), "p31-b".to_string())] {
            break;
        }
        assert!(Instant::now() < deadline, "b not silenced: {v:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Left out of the file: kept, unless pruned.
    assert!(apply(file(false), false, false).await.is_empty());
    let id = file_silence_id("quiet-b");
    assert_eq!(
        apply(file(false), true, true).await,
        [format!("silence {id}: removed")]
    );
    apply(file(false), false, true).await;
    let AdminResponse::AlertTuning(v) = client.call(AdminOp::AlertTuning).await.unwrap() else {
        panic!()
    };
    assert!(v.silences.iter().all(|s| s.id != id), "{v:?}");

    client.close().await;
    cluster.shutdown().await.unwrap();
}
