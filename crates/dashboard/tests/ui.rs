//! The dashboard against an in-process cluster, on every CI platform (the
//! browser tests in `e2e/` run on Linux only): sign-in link, pages, the
//! JSON API submitting a job and downloading its outputs as a zip, and
//! the request guards; and (phase 15) `--api-only` over TLS with failed
//! sign-ins rate limited and `?wait=` long polling.

use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use pumasi_agent::{Agent, AgentConfig};
use pumasi_cluster::{Cluster, ClusterConfig};
use pumasi_dashboard::UiConfig;
use pumasi_proto::net::NetMode;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

const TICKER: &[u8] = include_bytes!("../e2e/fixtures/ticker.wasm");

fn tempdir(tag: &str) -> PathBuf {
    use std::hash::{BuildHasher, RandomState};
    let n = RandomState::new().hash_one(SystemTime::now());
    let p = std::env::temp_dir().join(format!("pumasi-ui-test-{tag}-{n:x}"));
    std::fs::create_dir_all(&p).unwrap();
    p
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.text()))
    }
}

/// One HTTP/1.1 request on its own connection.
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> Resp {
    let s = TcpStream::connect(addr).await.unwrap();
    send(s, addr, method, path, headers, body).await
}

/// The same over TLS, trusting the test certificate.
async fn request_tls(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> Resp {
    use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(TEST_CERT).unwrap())
        .unwrap();
    let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = TcpStream::connect(addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    send(tls, addr, method, path, headers, body).await
}

/// The CA that signed `fixtures/test-cert.pem` (for localhost and
/// 127.0.0.1, valid until 2126).
const TEST_CERT: &[u8] = include_bytes!("fixtures/test-ca.pem");

async fn send(
    mut s: impl AsyncRead + AsyncWrite + Unpin,
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> Resp {
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\nContent-Length: {}\r\n",
        addr.port(),
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    // A TLS peer may close without close_notify; what arrived is enough.
    let _ = s.read_to_end(&mut buf).await;
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let mut lines = head.lines();
    let status = lines.next().unwrap()[9..12].parse().unwrap();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let mut body = buf[split + 4..].to_vec();
    let chunked = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked"));
    if chunked {
        let mut out = Vec::new();
        let mut rest = &body[..];
        loop {
            let nl = rest.windows(2).position(|w| w == b"\r\n").unwrap();
            let n = usize::from_str_radix(std::str::from_utf8(&rest[..nl]).unwrap().trim(), 16)
                .unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&rest[nl + 2..nl + 2 + n]);
            rest = &rest[nl + 2 + n + 2..];
        }
        body = out;
    }
    Resp {
        status,
        headers,
        body,
    }
}

async fn start_cluster() -> (
    Cluster,
    tokio::task::JoinHandle<anyhow::Result<pumasi_agent::AgentExit>>,
) {
    let mut config = ClusterConfig::new(tempdir("cp"));
    config.name = "ui-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    let cluster = Cluster::start(config).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let agent = Agent::new(AgentConfig {
        data_dir: tempdir("agent"),
        name: "ui-node".into(),
        slots: Some(2),
        net: NetMode::Local,
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
        max_cpu_percent: None,
        availability: None,
        bench: None,
    })
    .await
    .unwrap();
    let agent_task = tokio::spawn(agent.join(ticket));
    for _ in 0..200 {
        if cluster.status().nodes.iter().any(|n| n.online) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (cluster, agent_task)
}

/// A multipart job submission with the ticker module.
fn job_form(boundary: &str, name: &str) -> Vec<u8> {
    let mut body = Vec::new();
    let job = format!(
        "name = \"{name}\"\nmodule = \"ticker.wasm\"\nargs = [\"1\"]\nshards = 2\ndeterministic = false\n"
    );
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"job\"\r\n\r\n{job}\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"ticker.wasm\"\r\nContent-Type: application/wasm\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(TICKER);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_pages_api_and_guards() {
    let (cluster, agent_task) = start_cluster().await;
    let admin = cluster.admin_ticket();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ui = pumasi_dashboard::start_on(
        listener,
        UiConfig {
            ticket: admin.clone(),
            net: NetMode::Local,
            listen: "127.0.0.1:0".parse().unwrap(),
            login_only: false,
            upload_limit: 64 << 20,
            api_only: false,
            tls: None,
            real_ip_header: None,
        },
    )
    .await
    .unwrap();
    let addr = ui.addr();
    let url = ui.url();
    let token_path = &url[url.find("/login").unwrap()..];

    // Pages need a session; the printed link gives one.
    let r = request(addr, "GET", "/", &[], b"").await;
    assert_eq!(r.status, 303);
    assert_eq!(r.header("location"), Some("/login"));
    assert_eq!(
        request(addr, "GET", "/login?token=wrong", &[], b"")
            .await
            .status,
        401
    );
    let r = request(addr, "GET", token_path, &[], b"").await;
    assert_eq!(r.status, 303, "{}", r.text());
    let cookie = r
        .header("set-cookie")
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert!(r.header("set-cookie").unwrap().contains("HttpOnly"));
    let page = request(addr, "GET", "/", &[("Cookie", cookie.clone())], b"").await;
    assert_eq!(page.status, 200);
    let html = page.text();
    assert!(html.contains("ui-cluster"), "{html}");
    assert!(
        page.header("content-security-policy")
            .unwrap()
            .contains("script-src 'self'")
    );
    for path in [
        "/nodes",
        "/jobs",
        "/storage",
        "/cu",
        "/access",
        "/prices",
        "/events",
        "/jobs/new",
    ] {
        let r = request(addr, "GET", path, &[("Cookie", cookie.clone())], b"").await;
        assert_eq!(r.status, 200, "{path}: {}", r.text());
    }
    assert!(
        request(addr, "GET", "/nodes", &[("Cookie", cookie.clone())], b"")
            .await
            .text()
            .contains("ui-node")
    );

    // A session's POST without its CSRF token is refused.
    let r = request(
        addr,
        "POST",
        "/jobs/0000000000000001/cancel",
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await;
    assert_eq!(r.status, 403);
    // So is a foreign Host header.
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /healthz HTTP/1.1\r\nHost: evil.example\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 421"));

    // The API: no ticket, a ticket of the wrong form, then the admin's.
    assert_eq!(
        request(addr, "GET", "/api/v1/status", &[], b"")
            .await
            .status,
        401
    );
    let bad = request(
        addr,
        "GET",
        "/api/v1/status",
        &[("Authorization", "Bearer nope".into())],
        b"",
    )
    .await;
    assert_eq!(bad.status, 401);
    let auth = ("Authorization", format!("Bearer {admin}"));
    let status = request(
        addr,
        "GET",
        "/api/v1/status",
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(status.status, 200);
    assert_eq!(status.json()["cluster_name"], "ui-cluster");

    // Submit through the API with the module as a file.
    let boundary = "pumasi-test-boundary";
    let body = job_form(boundary, "api");
    let r = request(
        addr,
        "POST",
        "/api/v1/jobs",
        &[
            auth.clone(),
            (
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            ),
        ],
        &body,
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let id = r.json()["id"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 16);
    let mut state = String::new();
    for _ in 0..300 {
        let j = request(
            addr,
            "GET",
            &format!("/api/v1/jobs/{id}"),
            std::slice::from_ref(&auth),
            b"",
        )
        .await;
        state = j.json()["summary"]["state"].as_str().unwrap().to_string();
        if state == "Succeeded" || state == "Failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(state, "Succeeded");

    // The job page and a task's output.
    let r = request(
        addr,
        "GET",
        &format!("/jobs/{id}"),
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await;
    assert!(r.text().contains("result.txt"), "{}", r.text());
    let logs = request(
        addr,
        "GET",
        &format!("/api/v1/jobs/{id}/tasks/0/logs"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await
    .json();
    assert_eq!(logs["stdout"], "tick\ndone\n");
    assert_eq!(logs["done"], true);

    // Outputs as a zip, one file directly.
    let zip = request(
        addr,
        "GET",
        &format!("/api/v1/jobs/{id}/outputs.zip"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(zip.status, 200);
    assert_eq!(zip.header("content-type"), Some("application/zip"));
    assert_eq!(&zip.body[..4], b"PK\x03\x04");
    let text = String::from_utf8_lossy(&zip.body);
    assert!(text.contains("task-0/result.txt") && text.contains("task-1/result.txt"));
    assert_eq!(text.matches("pumasi dashboard ok\n").count(), 2);
    let one = request(
        addr,
        "GET",
        &format!("/jobs/{id}/tasks/1/files/result.txt"),
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await;
    assert_eq!(one.body, b"pumasi dashboard ok\n");
    assert!(
        one.header("content-disposition")
            .unwrap()
            .starts_with("attachment")
    );

    // Receipts in the export format.
    let receipts = request(
        addr,
        "GET",
        &format!("/api/v1/jobs/{id}/receipts"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await
    .json();
    assert!(receipts.as_array().unwrap().len() >= 2);

    ui.stop();
    agent_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_only_over_tls_with_rate_limit_and_long_poll() {
    let (cluster, agent_task) = start_cluster().await;
    let admin = cluster.admin_ticket();
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let tls = pumasi_dashboard::tls_config(
        &fixtures.join("test-cert.pem"),
        &fixtures.join("test-key.pem"),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ui = pumasi_dashboard::start_on(
        listener,
        UiConfig {
            ticket: admin.clone(),
            net: NetMode::Local,
            listen: "127.0.0.1:0".parse().unwrap(),
            login_only: false,
            upload_limit: 64 << 20,
            api_only: true,
            tls: Some(tls),
            real_ip_header: Some("CF-Connecting-IP".into()),
        },
    )
    .await
    .unwrap();
    let addr = ui.addr();
    // No sign-in link and HTTPS.
    assert_eq!(ui.url(), format!("https://127.0.0.1:{}/", addr.port()));
    assert_eq!(
        request_tls(addr, "GET", "/healthz", &[], b"").await.status,
        200
    );
    // No pages, no sign-in.
    for path in ["/", "/login", "/jobs", "/static/app.js"] {
        assert_eq!(
            request_tls(addr, "GET", path, &[], b"").await.status,
            404,
            "{path}"
        );
    }
    let r = request_tls(addr, "GET", "/api/v1/status", &[], b"").await;
    assert_eq!(r.status, 401);
    assert!(r.text().contains("Bearer"), "{}", r.text());
    // Behind a tunnel the Host is the public name: accepted.
    let r = request_tls(
        addr,
        "GET",
        "/api/openapi.json",
        &[("Host", "pumasi.example.com".into())],
        b"",
    )
    .await;
    assert_eq!(r.status, 200);

    let auth = ("Authorization", format!("Bearer {admin}"));
    let status = request_tls(
        addr,
        "GET",
        "/api/v1/status",
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(status.status, 200, "{}", status.text());
    let capacity = &status.json()["capacity"];
    assert!(
        capacity["nodes"].as_array().is_some_and(|n| !n.is_empty()),
        "{capacity}"
    );
    let who = request_tls(
        addr,
        "GET",
        "/api/v1/whoami",
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(who.status, 200);

    // Wrong tickets from one client: 401s, then 429 for everything from
    // it, while other clients carry on.
    let guesser = ("CF-Connecting-IP", "198.51.100.1".to_string());
    let mut other = cluster.admin_ticket();
    other.secret = [7; 32];
    for i in 0..10 {
        let bad = if i % 2 == 0 {
            format!("Bearer {other}")
        } else {
            "Bearer nope".to_string()
        };
        let r = request_tls(
            addr,
            "GET",
            "/api/v1/status",
            &[guesser.clone(), ("Authorization", bad)],
            b"",
        )
        .await;
        assert_eq!(r.status, 401, "attempt {i}: {}", r.text());
    }
    let r = request_tls(
        addr,
        "GET",
        "/api/v1/status",
        &[guesser.clone(), auth.clone()],
        b"",
    )
    .await;
    assert_eq!(r.status, 429, "{}", r.text());
    assert!(r.header("retry-after").is_some());
    let r = request_tls(
        addr,
        "GET",
        "/api/v1/status",
        &[("CF-Connecting-IP", "198.51.100.2".into()), auth.clone()],
        b"",
    )
    .await;
    assert_eq!(r.status, 200);

    // Submit, then one long poll until it is done.
    let boundary = "pumasi-test-boundary";
    let r = request_tls(
        addr,
        "POST",
        "/api/v1/jobs",
        &[
            auth.clone(),
            (
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            ),
        ],
        &job_form(boundary, "remote"),
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let id = r.json()["id"].as_str().unwrap().to_string();
    let r = request_tls(
        addr,
        "GET",
        &format!("/api/v1/jobs/{id}?wait=60s"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    let job = r.json();
    assert_eq!(job["summary"]["state"], "Succeeded", "{job}");
    assert_eq!(job["submitted_by"], "cluster admin", "{job}");
    let r = request_tls(
        addr,
        "GET",
        &format!("/api/v1/jobs/{id}?wait=soon"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(r.status, 400);

    ui.stop();
    agent_task.abort();
}
