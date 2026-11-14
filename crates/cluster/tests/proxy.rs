//! The CLI behind an HTTPS proxy (1.12): a client whose only way out is an
//! HTTP proxy with `CONNECT` reaches a control plane through the cluster's
//! own relay, trusting the relay's certificate from `PUMASI_CA_FILE`.
//!
//! Its own test binary: it sets process-wide environment variables.

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::EndpointAddr;
use iroh_relay::server::{CertConfig, RelayConfig, Server, ServerConfig, TlsConfig};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    msg::{AdminOp, AdminResponse},
    net::{CA_FILE_ENV, NetMode, PROXY_ENV, RelayUrl},
    ticket::AdminTicket,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn tempdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "pumasi-proxy-{tag}-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A CA, and a certificate for `localhost` it signed.
fn certs(
    dir: &std::path::Path,
) -> (
    PathBuf,
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name
        .push(rcgen::DnType::CommonName, "pumasi test proxy CA");
    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca, ca_key);
    let key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let cert = leaf.signed_by(&key, &issuer).unwrap();
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    (
        ca_path,
        cert.der().clone(),
        rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
    )
}

/// A minimal HTTP proxy that only does `CONNECT`, recording each target.
async fn connect_proxy() -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut client, _)) = listener.accept().await else {
                return;
            };
            let log = log.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") && head.len() < 8192 {
                    if client.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    head.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&head).to_string();
                let mut words = head.split_whitespace();
                let (Some("CONNECT"), Some(target)) = (words.next(), words.next()) else {
                    let _ = client
                        .write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                        .await;
                    return;
                };
                log.lock().unwrap().push(target.to_string());
                let Ok(mut upstream) = TcpStream::connect(target).await else {
                    let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                    return;
                };
                if client
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    (addr, seen)
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_through_a_connect_proxy() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempdir("main");
    let (ca, cert, key) = certs(&dir);

    // The cluster's own relay, with a certificate no public CA signed.
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .unwrap();
    let mut relay = RelayConfig::new((Ipv4Addr::LOCALHOST, 0));
    relay.tls = Some(TlsConfig::new(
        (Ipv4Addr::LOCALHOST, 0),
        CertConfig::Manual { server_config },
    ));
    let mut config = ServerConfig::default();
    config.relay = Some(relay);
    let relay = Server::spawn(config).await.unwrap();
    let relay_url: RelayUrl = format!("https://localhost:{}", relay.https_addr().unwrap().port())
        .parse()
        .unwrap();

    // The control plane goes to the relay directly and trusts its CA.
    // SAFETY: this test binary runs this one test; nothing else reads the
    // environment while it is changed.
    unsafe {
        std::env::set_var(CA_FILE_ENV, &ca);
        std::env::set_var(PROXY_ENV, "off");
    }
    let mut cc = ClusterConfig::new(dir.join("cluster"));
    cc.name = "proxy-test".into();
    cc.net = NetMode::Public;
    cc.relays = vec![relay_url.clone()];
    cc.bind = Some("127.0.0.1:0".parse().unwrap());
    let cluster = Cluster::start(cc).await.unwrap();
    let full = cluster.admin_ticket();
    let id = full.cluster.id;
    // Wait for the control plane to be on its relay.
    for _ in 0..100 {
        if cluster.admin_ticket().cluster.relay_urls().next().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A ticket without the control plane's IP addresses: like a sandbox
    // that cannot reach them, the client has only the relay.
    let ticket = AdminTicket {
        cluster: EndpointAddr::new(id).with_relay_url(relay_url.clone()),
        secret: full.secret,
        replicas: Vec::new(),
    };

    // The client: only the proxy.
    let (proxy, seen) = connect_proxy().await;
    unsafe {
        std::env::set_var(PROXY_ENV, format!("http://{proxy}"));
    }
    let client = tokio::time::timeout(
        Duration::from_secs(60),
        AdminClient::connect(&ticket, NetMode::Public),
    )
    .await
    .expect("connecting through the proxy timed out")
    .expect("connect through the proxy");
    let AdminResponse::Status(s) = client.call(AdminOp::Status).await.unwrap() else {
        panic!("unexpected response");
    };
    assert_eq!(s.cluster_name, "proxy-test");
    let targets = seen.lock().unwrap().clone();
    let want = format!("localhost:{}", relay_url.port().unwrap());
    assert!(
        targets.contains(&want),
        "the proxy saw no CONNECT to the relay ({want}): {targets:?}"
    );
    client.close().await;

    // Without the CA the relay's certificate is refused: the CA file is
    // what made it work.
    unsafe {
        std::env::remove_var(CA_FILE_ENV);
    }
    let refused = tokio::time::timeout(
        Duration::from_secs(10),
        AdminClient::connect(&ticket, NetMode::Public),
    )
    .await;
    assert!(
        !matches!(refused, Ok(Ok(_))),
        "connected through a relay whose CA is not trusted"
    );
    cluster.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
