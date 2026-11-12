//! The cluster file store end to end: datasets uploaded with the CLI's
//! client, read by sharded jobs on several nodes, job outputs written back
//! to the store, downloads that resume, storage billing, garbage
//! collection and restarts.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig, transfer::PARTIAL_DIR};
use pumasi_proto::{
    economy::{CuRates, Policy},
    job::{JobFile, JobId, JobSubmission},
    msg::{AdminOp, AdminResponse, JobState, JobView, LedgerEntry, LedgerKind},
    net::NetMode,
    store::{DatasetView, MountRequest, MountSource, StoreTarget},
    ticket::JoinTicket,
};
use tokio::task::JoinHandle;

/// `copy IN [OUT]`: reads `/input/IN` and writes it to `/output/OUT` in
/// 64 KiB chunks, printing the first 16 bytes to stdout.
const COPY_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "path_open" (func $open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_read" (func $read (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 2)
  (func $strlen (param $p i32) (result i32) (local $n i32)
    (block $d (loop $l
      (br_if $d (i32.eqz (i32.load8_u (i32.add (local.get $p) (local.get $n)))))
      (local.set $n (i32.add (local.get $n) (i32.const 1)))
      (br $l)))
    (local.get $n))
  (func (export "_start")
    (local $argc i32) (local $p i32) (local $first i32) (local $n i32)
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $args_get (i32.const 64) (i32.const 1024)))
    (local.set $argc (i32.load (i32.const 0)))
    (local.set $p (i32.load (i32.const 68)))
    (if (call $open (i32.const 3) (i32.const 0) (local.get $p) (call $strlen (local.get $p))
          (i32.const 0) (i64.const 2) (i64.const 0) (i32.const 0) (i32.const 200))
      (then (call $exit (i32.const 10))))
    (if (i32.ge_u (local.get $argc) (i32.const 3)) (then
      (local.set $p (i32.load (i32.const 72)))
      (if (call $open (i32.const 4) (i32.const 0) (local.get $p) (call $strlen (local.get $p))
            (i32.const 9) (i64.const 536870911) (i64.const 536870911) (i32.const 0) (i32.const 204))
        (then (call $exit (i32.const 11))))))
    (local.set $first (i32.const 1))
    (block $done (loop $l
      (i32.store (i32.const 300) (i32.const 65536))
      (i32.store (i32.const 304) (i32.const 65536))
      (if (call $read (i32.load (i32.const 200)) (i32.const 300) (i32.const 1) (i32.const 308))
        (then (call $exit (i32.const 12))))
      (local.set $n (i32.load (i32.const 308)))
      (br_if $done (i32.eqz (local.get $n)))
      (if (local.get $first) (then
        (local.set $first (i32.const 0))
        (i32.store (i32.const 320) (i32.const 65536))
        (i32.store (i32.const 324)
          (select (local.get $n) (i32.const 16) (i32.lt_u (local.get $n) (i32.const 16))))
        (drop (call $write (i32.const 1) (i32.const 320) (i32.const 1) (i32.const 328)))))
      (if (i32.ge_u (local.get $argc) (i32.const 3)) (then
        (i32.store (i32.const 320) (i32.const 65536))
        (i32.store (i32.const 324) (local.get $n))
        (if (call $write (i32.load (i32.const 204)) (i32.const 320) (i32.const 1) (i32.const 328))
          (then (call $exit (i32.const 13))))))
      (br $l)))
    (call $exit (i32.const 0))))
"#;

const MIB: usize = 1 << 20;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-store-{tag}-{:016x}", rand_u64()));
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

/// Bytes that differ per seed and do not repeat (so nothing dedups by
/// accident).
fn data(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn config(dir: PathBuf) -> ClusterConfig {
    let mut config = ClusterConfig::new(dir);
    config.name = "store-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    config.gc_interval = Duration::from_secs(1);
    config.storage_bill_every = Duration::from_secs(1);
    config
}

struct RunningAgent {
    endpoint: iroh::Endpoint,
    handle: JoinHandle<anyhow::Result<AgentExit>>,
}

impl RunningAgent {
    async fn kill(self) {
        self.handle.abort();
        self.endpoint.close().await;
    }
}

async fn spawn_agent(name: &str, ticket: JoinTicket) -> RunningAgent {
    let agent = Agent::new(AgentConfig {
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
        store_cache_bytes: Some(256 * MIB as u64),
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

async fn call(client: &AdminClient, op: AdminOp) -> AdminResponse {
    match client.call(op).await.unwrap() {
        AdminResponse::Error(e) => panic!("admin call failed: {e}"),
        r => r,
    }
}

async fn submit(client: &AdminClient, sub: JobSubmission) -> JobId {
    match call(client, AdminOp::Submit(Box::new(sub))).await {
        AdminResponse::Submitted { id } => id,
        other => panic!("unexpected {other:?}"),
    }
}

async fn job(client: &AdminClient, id: JobId) -> JobView {
    match call(
        client,
        AdminOp::Job {
            id,
            with_output: true,
        },
    )
    .await
    {
        AdminResponse::Job(v) => *v,
        other => panic!("unexpected {other:?}"),
    }
}

/// Waits for the job to end and, if it stored outputs, for them to become
/// a dataset.
async fn wait_job(client: &AdminClient, id: JobId) -> JobView {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let view = job(client, id).await;
        let stores = view.tasks.iter().any(|t| t.stored.is_some());
        if view.summary.state.is_terminal() && (!stores || view.output_dataset.is_some()) {
            return view;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job did not finish: {view:#?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn datasets(client: &AdminClient, name: Option<&str>) -> Vec<DatasetView> {
    match call(
        client,
        AdminOp::StoreList {
            name: name.map(String::from),
        },
    )
    .await
    {
        AdminResponse::Datasets(d) => d,
        other => panic!("unexpected {other:?}"),
    }
}

async fn ledger(client: &AdminClient, account: &str) -> Vec<LedgerEntry> {
    match call(
        client,
        AdminOp::Ledger {
            account: Some(account.into()),
            limit: 1000,
        },
    )
    .await
    {
        AdminResponse::Ledger(l) => l,
        other => panic!("unexpected {other:?}"),
    }
}

/// Rates that make a few seconds of a few MiB cost visible CU.
async fn expensive_storage(client: &AdminClient) {
    call(
        client,
        AdminOp::SetPolicy(Policy {
            rates: CuRates {
                storage_gib_month: 10_000_000_000_000,
                egress_gib: 1_000_000_000,
                ..CuRates::default()
            },
            ..Policy::default()
        }),
    )
    .await;
    call(
        client,
        AdminOp::Deposit {
            account: "alice".into(),
            amount: 1_000_000_000,
            memo: "test".into(),
        },
    )
    .await;
}

fn load_job(dir: &Path, toml: &str) -> (JobSubmission, Vec<(String, PathBuf)>) {
    std::fs::write(dir.join("copy.wasm"), wat::parse_str(COPY_WAT).unwrap()).unwrap();
    let path = dir.join("job.toml");
    std::fs::write(&path, toml).unwrap();
    JobFile::load_split(&path, 16 * MIB as u64).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datasets_feed_sharded_jobs_and_outputs_come_back() {
    init_tracing();
    let cluster_dir = tempdir("c");
    let cluster = Cluster::start(config(cluster_dir.clone())).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", ticket.clone()).await;
    let b = spawn_agent("b", ticket).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    expensive_storage(&client).await;

    // A dataset of four shards.
    let src = tempdir("src");
    let shards: Vec<Vec<u8>> = (0..4).map(|i| data(i, 3 * MIB + 1234)).collect();
    let mut files = Vec::new();
    for (i, s) in shards.iter().enumerate() {
        let p = src.join(format!("corpus/shard-{i:02}.bin"));
        write(&p, s);
        files.push((format!("shard-{i:02}.bin"), p));
    }
    let (stored, view) = client
        .store_put(&files, Some("corpus".into()), Some("alice".into()), |_| {})
        .await
        .unwrap();
    let view = view.unwrap();
    assert_eq!((view.name.as_str(), view.version), ("corpus", 1));
    assert_eq!(view.files, 4);
    assert_eq!(stored.size, 4 * (3 * MIB as u64 + 1234));
    // The same files again make no new version.
    let (_, again) = client
        .store_put(&files, Some("corpus".into()), Some("alice".into()), |_| {})
        .await
        .unwrap();
    assert_eq!(again.unwrap().version, 1);

    // Each task reads its own shard; outputs go to dataset `results`.
    let jobdir = tempdir("job");
    let (mut sub, upload) = load_job(
        &jobdir,
        r#"
name = "shards"
module = "copy.wasm"
args = ["corpus/shard-{shard:02}.bin", "out.bin"]
files = ["store:corpus/shard-{shard:02}.bin"]
shards = 4
output = "store:results"
"#,
    );
    assert!(upload.is_empty());
    sub.account = Some("alice".into());
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(view.output_dataset.as_deref(), Some("results@1"));
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(t.stdout.as_deref(), Some(&shards[i][..16]), "task {i}");
        let st = t.stored.expect("stored output");
        assert_eq!(st.size, shards[i].len() as u64);
    }
    let nodes: std::collections::HashSet<_> =
        view.tasks.iter().filter_map(|t| t.node.clone()).collect();
    assert_eq!(nodes.len(), 2, "shards should be read on both nodes");

    let out = tempdir("out");
    let (n, bytes) = client
        .store_get(
            StoreTarget::Dataset {
                name: "results".into(),
                version: None,
            },
            &out,
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(n, 4);
    assert_eq!(bytes, 4 * (3 * MIB as u64 + 1234));
    for (i, s) in shards.iter().enumerate() {
        let got = std::fs::read(out.join(format!("task-{i}/out.bin"))).unwrap();
        assert!(got == *s, "task {i} output differs");
    }
    assert!(!out.join(PARTIAL_DIR).exists());

    // Local files too large to send with the job go through the store,
    // and an output over the inline limit is stored on its own.
    let big = data(99, 40 * MIB);
    write(&jobdir.join("big.bin"), &big);
    write(&jobdir.join("small.bin"), b"0123456789abcdefXYZ");
    let (mut sub, upload) = load_job(
        &jobdir,
        r#"
name = "big"
module = "copy.wasm"
files = ["big.bin", "small.bin"]
[[task]]
args = ["big.bin", "big-out.bin"]
[[task]]
args = ["small.bin", "small-out.bin"]
"#,
    );
    assert_eq!(upload.len(), 2, "{upload:?}");
    assert!(sub.files.is_empty());
    let (input, _) = client
        .store_put(&upload, None, Some("alice".into()), |_| {})
        .await
        .unwrap();
    sub.mounts.insert(
        0,
        MountRequest {
            source: MountSource::Upload { root: input.root },
            select: None,
            at: String::new(),
        },
    );
    sub.account = Some("alice".into());
    let big_id = submit(&client, sub).await;
    let view = wait_job(&client, big_id).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let stored = view.tasks[0].stored.expect("a 40 MiB output is stored");
    assert_eq!(stored.size, big.len() as u64);
    assert!(
        view.tasks[1].stored.is_none(),
        "a small output stays inline"
    );
    assert_eq!(
        view.tasks[1].output_files,
        vec![("small-out.bin".into(), 19)]
    );
    assert_eq!(
        view.tasks[1].stdout.as_deref(),
        Some(&b"0123456789abcdef"[..])
    );
    let job_ds = view.output_dataset.clone().unwrap();
    assert!(job_ds.starts_with("job-"), "{job_ds}");

    let out = tempdir("bigout");
    client
        .store_get(StoreTarget::Job { id: big_id }, &out, |_| {})
        .await
        .unwrap();
    assert!(std::fs::read(out.join("task-0/big-out.bin")).unwrap() == big);

    // Storage and downloads were billed to alice.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let entries = ledger(&client, "alice").await;
    let storage: Vec<_> = entries
        .iter()
        .filter(|e| e.kind == LedgerKind::Storage)
        .collect();
    assert!(!storage.is_empty(), "{entries:#?}");
    assert!(storage.iter().all(|e| e.amount < 0));
    let egress: Vec<_> = entries
        .iter()
        .filter(|e| e.kind == LedgerKind::Egress)
        .collect();
    assert!(egress.len() >= 2, "{entries:#?}");
    assert!(egress.iter().all(|e| e.amount < 0));

    // Everything survives a restart of the control plane.
    a.kill().await;
    b.kill().await;
    client.close().await;
    cluster.shutdown().await.unwrap();
    let cluster = Cluster::start(config(cluster_dir.clone())).await.unwrap();
    let client = admin(&cluster).await;
    let all = datasets(&client, None).await;
    let names: Vec<_> = all.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"corpus"), "{names:?}");
    assert!(names.contains(&"results"), "{names:?}");
    assert!(
        names.contains(&job_ds.split('@').next().unwrap()),
        "{names:?}"
    );
    let out = tempdir("again");
    let (n, _) = client
        .store_get(
            StoreTarget::Dataset {
                name: "corpus".into(),
                version: Some(1),
            },
            &out,
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(n, 4);
    assert!(std::fs::read(out.join("shard-02.bin")).unwrap() == shards[2]);

    // Removing every dataset gives the disk back.
    let before = cluster.store_disk_bytes();
    assert!(before > 50 * MIB as u64, "{before}");
    for d in &all {
        call(
            &client,
            AdminOp::StoreRemove {
                name: d.name.clone(),
                version: None,
            },
        )
        .await;
    }
    assert!(datasets(&client, None).await.is_empty());
    let mut after = before;
    for _ in 0..100 {
        after = cluster.store_disk_bytes();
        if after < 8 * MIB as u64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(after < 8 * MIB as u64, "disk use {before} -> {after}");

    client.close().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_download_resumes_where_it_stopped() {
    init_tracing();
    let cluster = Cluster::start(config(tempdir("c"))).await.unwrap();
    let client = admin(&cluster).await;
    expensive_storage(&client).await;

    let src = tempdir("src");
    let (one, two) = (data(1, 4 * MIB), data(2, 4 * MIB));
    write(&src.join("one.bin"), &one);
    write(&src.join("two.bin"), &two);
    let files = vec![
        ("one.bin".to_string(), src.join("one.bin")),
        ("two.bin".to_string(), src.join("two.bin")),
    ];
    client
        .store_put(&files, Some("pair".into()), Some("alice".into()), |_| {})
        .await
        .unwrap();

    // A download that stopped after the first file: its partial store has
    // the collection's root and names and all of `one.bin`.
    let dest = tempdir("dest");
    {
        use pumasi_proto::blobs;
        let full = blobs::open_store(&tempdir("full"), None).await.unwrap();
        let (tt, _, _) = blobs::import_files(&full, &files, false).await.unwrap();
        let members = blobs::hash_seq_members(&full, tt.hash()).await;
        assert_eq!(members.len(), 4, "root, names, one, two");
        let partial = blobs::open_store(&dest.join(PARTIAL_DIR), None)
            .await
            .unwrap();
        for h in &members[..3] {
            let bytes = full.blobs().get_bytes(*h).await.unwrap();
            partial.blobs().add_bytes(bytes).await.unwrap();
        }
        partial
            .tags()
            .set("keep", blobs::HashAndFormat::hash_seq(tt.hash()))
            .await
            .unwrap();
        partial.shutdown().await.unwrap();
        full.shutdown().await.unwrap();
    }

    let (n, bytes) = client
        .store_get(
            StoreTarget::Dataset {
                name: "pair".into(),
                version: None,
            },
            &dest,
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!((n, bytes), (2, 8 * MIB as u64));
    assert!(std::fs::read(dest.join("one.bin")).unwrap() == one);
    assert!(std::fs::read(dest.join("two.bin")).unwrap() == two);
    assert!(!dest.join(PARTIAL_DIR).exists());

    // Only the missing half crossed the network (and was billed).
    let mut memos = Vec::new();
    for _ in 0..50 {
        memos = ledger(&client, "alice")
            .await
            .into_iter()
            .filter(|e| e.kind == LedgerKind::Egress)
            .map(|e| e.memo)
            .collect();
        if !memos.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(memos, vec!["4.0 MiB downloaded".to_string()]);

    client.close().await;
    cluster.shutdown().await.unwrap();
}
