//! Copies of the file store on several machines (1.8): dataset versions
//! replicated to nodes labelled for storage, writes acknowledged once
//! they are on their copies, a control plane that lost its `blobs`
//! directory getting every dataset back from the nodes, replicas moved off
//! a removed node, and scrubs that find damaged data.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    manage::NodeEdit,
    monitor::EventView,
    msg::{AdminOp, AdminResponse},
    net::NetMode,
    replica::{ReplicaState, ReplicationView},
    store::StoreTarget,
    ticket::JoinTicket,
};
use tokio::task::JoinHandle;

const MIB: usize = 1 << 20;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-rep-{tag}-{:016x}", rand_u64()));
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

/// A UDP port nothing listens on now, so a restarted control plane comes
/// back at the address its agents know.
fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(dir: PathBuf, port: u16) -> ClusterConfig {
    let mut config = ClusterConfig::new(dir);
    config.name = "replica-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some(format!("127.0.0.1:{port}").parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    config.gc_interval = Duration::from_secs(1);
    config.repair_every = Duration::from_secs(1);
    config.replica_ack_timeout = Duration::from_secs(60);
    config
}

struct RunningAgent {
    endpoint: iroh::Endpoint,
    handle: JoinHandle<anyhow::Result<AgentExit>>,
    dir: PathBuf,
}

impl RunningAgent {
    async fn kill(self) {
        self.handle.abort();
        self.endpoint.close().await;
    }
}

async fn spawn_agent(name: &str, ticket: JoinTicket) -> RunningAgent {
    let dir = tempdir(name);
    let agent = Agent::new(AgentConfig {
        data_dir: dir.clone(),
        name: name.into(),
        slots: Some(1),
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
    RunningAgent {
        endpoint,
        handle,
        dir,
    }
}

async fn admin(cluster: &Cluster) -> AdminClient {
    AdminClient::connect(&cluster.admin_ticket(), NetMode::Local)
        .await
        .unwrap()
}

async fn wait_online(cluster: &Cluster, n: usize) {
    for _ in 0..400 {
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

async fn replication(client: &AdminClient, name: &str) -> ReplicationView {
    match call(
        client,
        AdminOp::Replication {
            name: Some(name.into()),
        },
    )
    .await
    {
        AdminResponse::Replication(r) => *r,
        other => panic!("unexpected {other:?}"),
    }
}

/// Polls the replication of `name` until `ok` holds.
async fn wait_for(
    client: &AdminClient,
    name: &str,
    what: &str,
    ok: impl Fn(&ReplicationView) -> bool,
) -> ReplicationView {
    let mut last = None;
    for _ in 0..300 {
        let r = replication(client, name).await;
        if ok(&r) {
            return r;
        }
        last = Some(r);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{what}: {last:#?}");
}

async fn events(client: &AdminClient) -> Vec<EventView> {
    match call(
        client,
        AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 1000,
        },
    )
    .await
    {
        AdminResponse::Events(e) => e,
        other => panic!("unexpected {other:?}"),
    }
}

async fn label(client: &AdminClient, node: &str, value: &str) {
    call(
        client,
        AdminOp::EditNode {
            node: node.into(),
            edit: Box::new(NodeEdit {
                set_labels: BTreeMap::from([("storage".to_string(), value.to_string())]),
                ..Default::default()
            }),
        },
    )
    .await;
}

fn holders(r: &ReplicationView) -> Vec<String> {
    let mut v: Vec<String> = r.datasets[0]
        .nodes
        .iter()
        .filter(|n| n.counted)
        .map(|n| n.name.clone())
        .collect();
    v.sort();
    v
}

fn write_files(dir: &Path, files: &[(&str, &[u8])]) -> Vec<(String, PathBuf)> {
    files
        .iter()
        .map(|(name, bytes)| {
            let p = dir.join(name);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, bytes).unwrap();
            (name.to_string(), p)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicas_bring_back_a_lost_store_and_leave_a_removed_node() {
    init_tracing();
    let cluster_dir = tempdir("c");
    let port = free_port();
    let cluster = Cluster::start(config(cluster_dir.clone(), port))
        .await
        .unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(600), None)
        .unwrap();
    let mut agents: BTreeMap<String, RunningAgent> = BTreeMap::new();
    for name in ["node-a", "node-b", "node-c"] {
        agents.insert(name.into(), spawn_agent(name, ticket.clone()).await);
    }
    wait_online(&cluster, 3).await;
    let client = admin(&cluster).await;

    // Without storage nodes a version is on the control plane only, and
    // the write does not wait.
    let src = tempdir("src");
    let big = data(1, 3 * MIB + 17);
    let small = b"hello replicas".to_vec();
    let files = write_files(&src, &[("big.bin", &big), ("dir/small.txt", &small)]);
    let (_, view) = client
        .store_put(&files, Some("corpus".into()), None, |_| {})
        .await
        .unwrap();
    assert_eq!(view.unwrap().version, 1);
    let r = replication(&client, "corpus").await;
    assert_eq!((r.datasets[0].copies, r.datasets[0].factor), (1, 2), "{r:#?}");
    assert_eq!(r.under, 1);

    // Two storage nodes: the version gets its second copy on one of them.
    label(&client, "node-a", "1G").await;
    label(&client, "node-b", "2G").await;
    let r = wait_for(&client, "corpus", "a node replica", |r| {
        r.datasets[0].copies == 2
    })
    .await;
    assert_eq!(holders(&r).len(), 1);
    assert_eq!(r.nodes.len(), 2, "{r:#?}");

    // A new version is acknowledged only once it is on both copies.
    let more = data(2, MIB);
    let files2 = write_files(
        &src,
        &[("big.bin", &big), ("dir/small.txt", &small), ("more.bin", &more)],
    );
    let (_, view) = client
        .store_put(&files2, Some("corpus".into()), None, |_| {})
        .await
        .unwrap();
    assert_eq!(view.unwrap().version, 2);
    let r = replication(&client, "corpus").await;
    let v2 = r.datasets.iter().find(|d| d.version == 2).unwrap();
    assert_eq!(v2.copies, 2, "acknowledged before its copy: {r:#?}");

    // Three copies: both storage nodes hold every version.
    call(
        &client,
        AdminOp::SetReplication {
            name: Some("corpus".into()),
            factor: Some(3),
        },
    )
    .await;
    let r = wait_for(&client, "corpus", "three copies", |r| {
        r.datasets.iter().all(|d| d.copies == 3)
    })
    .await;
    assert_eq!(holders(&r), ["node-a", "node-b"]);
    assert!(r.datasets.iter().all(|d| d.own_factor));

    // node-a goes away for good: its replicas move to node-c once it is a
    // storage node too.
    label(&client, "node-c", "512M").await;
    let a = agents.remove("node-a").unwrap();
    let a_id = a.endpoint.id().to_string();
    a.kill().await;
    call(&client, AdminOp::RemoveNode { id: a_id }).await;
    let r = wait_for(&client, "corpus", "replicas on node-c", |r| {
        r.datasets
            .iter()
            .all(|d| d.copies == 3 && d.nodes.iter().all(|n| n.name != "node-a"))
    })
    .await;
    assert_eq!(holders(&r), ["node-b", "node-c"]);
    assert_eq!(r.nodes.len(), 2, "{r:#?}");

    // The control plane loses its whole store: it comes back empty and
    // fetches every version from the nodes.
    client.close().await;
    cluster.shutdown().await.unwrap();
    std::fs::remove_dir_all(cluster_dir.join("blobs")).unwrap();
    let cluster = Cluster::start(config(cluster_dir.clone(), port))
        .await
        .unwrap();
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    let r = wait_for(&client, "corpus", "the store fetched back", |r| {
        r.leader_missing == 0 && r.datasets.iter().all(|d| d.leader_has && d.copies == 3)
    })
    .await;
    assert_eq!(r.datasets.len(), 2);
    let out = tempdir("out");
    let (n, _) = client
        .store_get(
            StoreTarget::Dataset {
                name: "corpus".into(),
                version: Some(2),
            },
            &out,
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(n, 3);
    assert!(std::fs::read(out.join("big.bin")).unwrap() == big);
    assert!(std::fs::read(out.join("more.bin")).unwrap() == more);
    assert_eq!(std::fs::read(out.join("dir/small.txt")).unwrap(), small);
    let ev = events(&client).await;
    assert!(
        ev.iter().any(|e| e.kind == "store.repaired"),
        "{:#?}",
        ev.iter().map(|e| &e.message).collect::<Vec<_>>()
    );

    // A scrub finds a damaged replica on node-c, which drops it.
    let c = &agents["node-c"];
    let mut data_files = Vec::new();
    find_data_files(&c.dir.join("store"), &mut data_files);
    assert!(!data_files.is_empty(), "no data files in node-c's store");
    for f in &data_files {
        let mut bytes = std::fs::read(f).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x5a;
        std::fs::write(f, bytes).unwrap();
    }
    call(
        &client,
        AdminOp::Scrub {
            node: Some("node-c".into()),
        },
    )
    .await;
    let mut found = false;
    for _ in 0..100 {
        if events(&client).await.iter().any(|e| e.kind == "store.damaged") {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "no store.damaged event");
    let r = wait_for(&client, "corpus", "node-c's damaged copy not counted", |r| {
        r.datasets.iter().any(|d| {
            d.nodes
                .iter()
                .any(|n| n.name == "node-c" && n.state == ReplicaState::Copying)
        })
    })
    .await;
    assert!(r.nodes.iter().any(|n| n.name == "node-c" && n.bad > 0), "{r:#?}");

    // Removing the dataset drops its replicas.
    call(
        &client,
        AdminOp::StoreRemove {
            name: "corpus".into(),
            version: None,
        },
    )
    .await;
    let mut empty = false;
    for _ in 0..100 {
        if let AdminResponse::Replication(r) =
            call(&client, AdminOp::Replication { name: None }).await
            && r.nodes.iter().all(|n| n.replicas == 0)
        {
            empty = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(empty, "replicas of a removed dataset stayed");

    client.close().await;
    for (_, a) in agents {
        a.kill().await;
    }
    cluster.shutdown().await.unwrap();
}

fn find_data_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            find_data_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "data") {
            out.push(p);
        }
    }
}
