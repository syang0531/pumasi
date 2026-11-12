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
            public_url: None,
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
            public_url: None,
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

    // A job file names only files it was sent with, never one on this
    // machine (internal review, 1.0).
    let secret = std::env::temp_dir().join(format!("pumasi-ui-secret-{}", std::process::id()));
    std::fs::write(&secret, "not for jobs").unwrap();
    let job = format!(
        "module = \"ticker.wasm\"\nstdin = \"{}\"\n",
        secret.display().to_string().replace('\\', "/")
    );
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"job\"\r\n\r\n{job}\r\n--{boundary}--\r\n"
    );
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
        body.as_bytes(),
    )
    .await;
    let _ = std::fs::remove_file(&secret);
    assert!(
        r.status == 400 && r.text().contains("inside its folder"),
        "{} {}",
        r.status,
        r.text()
    );

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

// ---------------------------------------------------------------------------
// Phase 17: SSO sign-in through the dashboard, and request limits
// ---------------------------------------------------------------------------

/// A stand-in OpenID Connect provider that signs everyone in as
/// alice@corp.example (verified), in group `ops`.
mod idp {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use axum::{
        Form, Json, Router,
        extract::{Query, State},
        http::{StatusCode, header},
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use serde_json::{Value, json};

    pub struct Idp {
        pub issuer: String,
        pkcs8: Vec<u8>,
        point: Vec<u8>,
        codes: HashMap<String, String>,
    }

    type S = Arc<Mutex<Idp>>;

    fn b64(b: &[u8]) -> String {
        data_encoding::BASE64URL_NOPAD.encode(b)
    }

    async fn discovery(State(s): State<S>) -> Json<Value> {
        let i = s.lock().unwrap().issuer.clone();
        Json(
            json!({"issuer": i, "authorization_endpoint": format!("{i}/authorize"),
                    "token_endpoint": format!("{i}/token"), "jwks_uri": format!("{i}/jwks")}),
        )
    }

    async fn jwks(State(s): State<S>) -> Json<Value> {
        let i = s.lock().unwrap();
        let (x, y) = i.point[1..].split_at(32);
        Json(json!({"keys": [{"kty": "EC", "crv": "P-256", "kid": "k", "x": b64(x), "y": b64(y)}]}))
    }

    async fn authorize(State(s): State<S>, Query(q): Query<HashMap<String, String>>) -> Response {
        let code = format!("c{}", rand::random::<u32>());
        s.lock()
            .unwrap()
            .codes
            .insert(code.clone(), q["nonce"].clone());
        let to = format!("{}?code={code}&state={}", q["redirect_uri"], q["state"]);
        (StatusCode::FOUND, [(header::LOCATION, to)]).into_response()
    }

    async fn token(State(s): State<S>, Form(f): Form<HashMap<String, String>>) -> Response {
        let mut i = s.lock().unwrap();
        let Some(nonce) = i.codes.remove(&f["code"]) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_grant"})),
            )
                .into_response();
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = json!({"iss": i.issuer, "aud": "dash", "sub": "1", "email": "alice@corp.example",
                            "email_verified": true, "groups": ["ops"], "nonce": nonce,
                            "iat": now, "exp": now + 300});
        let mut h = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
        h.kid = Some("k".into());
        let t = jsonwebtoken::encode(
            &h,
            &claims,
            &jsonwebtoken::EncodingKey::from_ec_der(&i.pkcs8),
        )
        .unwrap();
        Json(json!({"id_token": t})).into_response()
    }

    pub async fn start() -> SocketAddr {
        use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let s = Arc::new(Mutex::new(Idp {
            issuer: format!("http://{addr}"),
            pkcs8: pkcs8.as_ref().to_vec(),
            point: pair.public_key().as_ref().to_vec(),
            codes: HashMap::new(),
        }));
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/authorize", get(authorize))
            .route("/token", post(token))
            .with_state(s);
        tokio::spawn(async move { axum::serve(listener, app).await });
        addr
    }
}

/// The `name=value` of a Set-Cookie header named `name`.
/// One raw HTTP/1.1 exchange, for requests with a Host of our choosing.
async fn raw(addr: SocketAddr, request: &str) -> String {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf).await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// The full dashboard on loopback behind a tunnel: browsers send the
/// public name as Host and Origin, which `--public-url` makes acceptable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_behind_a_tunnel() {
    let (cluster, agent_task) = start_cluster().await;
    let admin = cluster.admin_ticket();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ui = pumasi_dashboard::start_on(
        listener,
        UiConfig {
            ticket: admin.clone(),
            net: NetMode::Local,
            listen: "127.0.0.1:0".parse().unwrap(),
            login_only: true,
            upload_limit: 64 << 20,
            api_only: false,
            tls: None,
            real_ip_header: Some("CF-Connecting-IP".into()),
            public_url: Some("https://Pumasi.Example.com/".into()),
        },
    )
    .await
    .unwrap();
    let addr = ui.addr();

    let page = raw(
        addr,
        "GET /login HTTP/1.1\r\nHost: pumasi.example.com\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    // Other names are still refused (DNS rebinding).
    let other = raw(
        addr,
        "GET /login HTTP/1.1\r\nHost: evil.example\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(other.starts_with("HTTP/1.1 421"), "{other}");

    // Signing in from the public page: same origin, and a Secure cookie.
    let body = format!("ticket={admin}");
    let signed = raw(
        addr,
        &format!(
            "POST /login HTTP/1.1\r\nHost: pumasi.example.com\r\nOrigin: https://pumasi.example.com\r\nCF-Connecting-IP: 203.0.113.7\r\nConnection: close\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    assert!(signed.starts_with("HTTP/1.1 303"), "{signed}");
    let cookie_line = signed
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("set-cookie:"))
        .unwrap();
    assert!(cookie_line.contains("Secure"), "{cookie_line}");

    // The JSON API answers bearer tickets on the same name.
    let api = raw(
        addr,
        &format!(
            "GET /api/v1/whoami HTTP/1.1\r\nHost: pumasi.example.com\r\nAuthorization: Bearer {admin}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(api.starts_with("HTTP/1.1 200"), "{api}");

    ui.stop();
    agent_task.abort();
    drop(cluster);
}

fn set_cookie(r: &Resp, name: &str) -> Option<String> {
    r.headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("set-cookie"))
        .map(|(_, v)| v.split(';').next().unwrap_or("").to_string())
        .find(|kv| kv.starts_with(&format!("{name}=")) && kv.len() > name.len() + 1)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sso_sign_in_and_request_limits() {
    use pumasi_proto::{
        manage::AccessRole,
        msg::AdminOp,
        sso::{Hidden, OidcSettings, RoleMap, SsoUpdate, Subject},
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let idp_addr = idp::start().await;
    let (cluster, agent_task) = start_cluster().await;
    let admin = cluster.admin_ticket();
    let client = pumasi_cluster::AdminClient::connect(&admin, NetMode::Local)
        .await
        .unwrap();
    let oidc = OidcSettings {
        name: "Corp".into(),
        issuer: format!("http://{idp_addr}"),
        client_id: "dash".into(),
        client_secret: Hidden("x".into()),
        scopes: vec!["email".into()],
        groups_claim: "groups".into(),
    };
    client
        .call(AdminOp::SsoSet(Box::new(SsoUpdate::Oidc(Some(Box::new(
            oidc,
        ))))))
        .await
        .unwrap();
    client
        .call(AdminOp::SsoSet(Box::new(SsoUpdate::Map(RoleMap {
            subject: "group:ops".parse::<Subject>().unwrap(),
            role: AccessRole::Viewer,
        }))))
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ui = pumasi_dashboard::start_on(
        listener,
        UiConfig {
            ticket: admin.clone(),
            net: NetMode::Local,
            listen: "127.0.0.1:0".parse().unwrap(),
            login_only: true,
            upload_limit: 64 << 20,
            api_only: false,
            tls: None,
            real_ip_header: None,
            public_url: None,
        },
    )
    .await
    .unwrap();
    let addr = ui.addr();

    // The sign-in page offers the provider.
    let page = request(addr, "GET", "/login", &[], b"").await;
    assert!(page.text().contains("Sign in with Corp"), "{}", page.text());

    // Off to the provider, with the state in a Lax cookie.
    let start = request(addr, "GET", "/sso/start", &[], b"").await;
    assert_eq!(start.status, 303, "{}", start.text());
    let raw = start
        .headers
        .iter()
        .find(|(k, v)| k.eq_ignore_ascii_case("set-cookie") && v.starts_with("pumasi_sso="))
        .unwrap()
        .1
        .clone();
    assert!(
        raw.contains("SameSite=Lax") && raw.contains("HttpOnly"),
        "{raw}"
    );
    let sso = set_cookie(&start, "pumasi_sso").unwrap();
    let to = start.header("location").unwrap().to_string();
    let redirect = format!(
        "redirect_uri=http%3A%2F%2F127.0.0.1%3A{}%2Fsso%2Fcallback",
        addr.port()
    );
    assert!(to.contains(&redirect), "{to}");
    let at_idp = request(
        idp_addr,
        "GET",
        to.strip_prefix(&format!("http://{idp_addr}")).unwrap(),
        &[],
        b"",
    )
    .await;
    assert_eq!(at_idp.status, 302);
    let back = at_idp.header("location").unwrap().to_string();
    let path = back
        .strip_prefix(&format!("http://127.0.0.1:{}", addr.port()))
        .unwrap()
        .to_string();

    // Coming back without this browser's state is refused.
    let foreign = request(
        addr,
        "GET",
        &path,
        &[("Cookie", "pumasi_sso=other".into())],
        b"",
    )
    .await;
    assert_eq!(foreign.status, 400);
    assert!(set_cookie(&foreign, "pumasi_session").is_none());

    // With it, a session (and a page that moves on, for the Strict cookie).
    let done = request(addr, "GET", &path, &[("Cookie", sso.clone())], b"").await;
    assert_eq!(done.status, 200, "{}", done.text());
    assert!(done.text().contains("http-equiv=\"refresh\""));
    let session = set_cookie(&done, "pumasi_session").unwrap();
    let home = request(
        addr,
        "GET",
        "/api/v1/whoami",
        &[("Cookie", session.clone())],
        b"",
    )
    .await;
    assert_eq!(home.status, 200, "{}", home.text());
    assert_eq!(home.json()["name"], "sso:alice@corp.example");
    assert_eq!(home.json()["role"], "Viewer");
    // The code was used up.
    let again = request(addr, "GET", &path, &[("Cookie", sso)], b"").await;
    assert_eq!(again.status, 401);

    // Bodies: a sign-in form larger than 1 MiB is refused before it is read.
    let big = vec![b'a'; (1 << 20) + 10];
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /login HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n",
        addr.port(),
        big.len()
    );
    s.write_all(head.as_bytes()).await.unwrap();
    let _ = s.write_all(&big).await;
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf).await;
    assert!(
        String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 413"),
        "{}",
        String::from_utf8_lossy(&buf[..buf.len().min(200)])
    );

    // A ticket that goes too fast is slowed down.
    let bearer = ("Authorization", format!("Bearer {admin}"));
    let mut limited = None;
    for _ in 0..250 {
        let r = request(
            addr,
            "GET",
            "/api/v1/whoami",
            std::slice::from_ref(&bearer),
            b"",
        )
        .await;
        if r.status == 429 {
            limited = Some(r);
            break;
        }
        assert_eq!(r.status, 200);
    }
    let limited = limited.expect("never slowed down");
    assert!(limited.header("retry-after").is_some());
    assert!(
        limited
            .text()
            .contains("too many requests with this ticket")
    );
    // Other tickets are not affected.
    let other = request(addr, "GET", "/api/v1/whoami", &[("Cookie", session)], b"").await;
    assert_eq!(other.status, 200);

    ui.stop();
    client.close().await;
    agent_task.abort();
    cluster.shutdown().await.unwrap();
}

/// A multipart job submission of one job.toml and nothing else.
fn toml_form(boundary: &str, job: &str) -> Vec<u8> {
    format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"job\"\r\n\r\n{job}\r\n--{boundary}--\r\n"
    )
    .into_bytes()
}

/// Release 1.4: an account's policy over HTTP (GET, and PATCH changing
/// only what the body names), `whoami` carrying it, `?validate=1`
/// answering like a real submission without submitting, and each task's
/// node name and times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_policy_validation_and_task_times() {
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
            public_url: None,
        },
    )
    .await
    .unwrap();
    let addr = ui.addr();
    let auth = ("Authorization", format!("Bearer {admin}"));
    let json = ("Content-Type", "application/json".to_string());
    let op = |body: serde_json::Value| {
        let headers = [auth.clone(), json.clone()];
        async move {
            let r = request(
                addr,
                "POST",
                "/api/v1/op",
                &headers,
                body.to_string().as_bytes(),
            )
            .await;
            assert_eq!(r.status, 200, "{}", r.text());
            r.json()
        }
    };
    let ticket = |name: &'static str, role: serde_json::Value| {
        let made = op(serde_json::json!({
            "CreateAccess": {"name": name, "role": role, "ttl_secs": null}
        }));
        async move {
            let t = made.await["ticket"].as_str().unwrap().to_string();
            ("Authorization", format!("Bearer {t}"))
        }
    };
    let lab = ticket(
        "lab scripts",
        serde_json::json!({"Submitter": {"account": "lab"}}),
    )
    .await;
    let viewer = ticket("watcher", serde_json::json!("Viewer")).await;
    op(serde_json::json!({"Deposit": {"account": "lab", "amount": 100_000, "memo": "test"}})).await;
    let policy = "/api/v1/accounts/lab/policy";
    let patch = |who: &(&'static str, String), body: serde_json::Value| {
        let headers = [who.clone(), json.clone()];
        async move { request(addr, "PATCH", policy, &headers, body.to_string().as_bytes()).await }
    };

    // An account nobody has touched is not there; a new one reads as
    // "everything allowed".
    let r = request(
        addr,
        "GET",
        "/api/v1/accounts/nobody/policy",
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(r.status, 404, "{}", r.text());
    let r = request(addr, "GET", policy, std::slice::from_ref(&auth), b"").await;
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(v["account"], "lab");
    assert_eq!(v["images"], serde_json::json!([]));
    assert_eq!(v["network"], "allow");
    assert_eq!(v["tolerations"], serde_json::Value::Null);
    assert_eq!(v["quota"]["slots"], serde_json::Value::Null);

    // PATCH sets what it names.
    let r = patch(
        &auth,
        serde_json::json!({
            "executors": ["wasm"],
            "images": ["ghcr.io/lab/*"],
            "network": "deny",
            "max_timeout_ms": 600_000,
            "quota": {"slots": 2},
        }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    // ... and nothing else: adding one image keeps the rest.
    let r = patch(
        &auth,
        serde_json::json!({"images": {"add": ["python:3.12-slim"]}}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(
        v["images"],
        serde_json::json!(["ghcr.io/lab/*", "python:3.12-slim"])
    );
    assert_eq!(v["executors"], serde_json::json!(["wasm"]));
    assert_eq!(v["network"], "deny");
    assert_eq!(v["max_timeout_ms"], 600_000);
    assert_eq!(v["quota"]["slots"], 2);
    // Add and remove in one call; null lifts a limit; the quota's other
    // fields stay.
    let r = patch(
        &auth,
        serde_json::json!({
            "images": {"add": ["node:22-slim"], "remove": ["python:3.12-slim"]},
            "max_timeout_ms": null,
            "tolerations": [],
            "quota": {"gpus": 1},
        }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(
        v["images"],
        serde_json::json!(["ghcr.io/lab/*", "node:22-slim"])
    );
    assert_eq!(v["max_timeout_ms"], serde_json::Value::Null);
    assert_eq!(v["tolerations"], serde_json::json!([]));
    assert_eq!(v["quota"]["slots"], 2);
    assert_eq!(v["quota"]["gpus"], 1);
    // What GET answers goes back in unchanged.
    let before = request(addr, "GET", policy, std::slice::from_ref(&auth), b"")
        .await
        .json();
    let r = patch(&auth, before.clone()).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json(), before);

    // Removing what is not there changes nothing, so a call can be
    // repeated.
    let r = patch(
        &auth,
        serde_json::json!({"images": {"add": ["node:22-slim"], "remove": ["alpine"]}}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json(), before);

    // Refused, and nothing changes: the last entry of a list, an unknown
    // field, another account's body, a ticket that is not an admin's.
    for (body, status, text) in [
        (
            serde_json::json!({"executors": {"remove": ["wasm"]}}),
            400,
            "would allow any",
        ),
        (serde_json::json!({"image": ["a"]}), 400, "image"),
        (serde_json::json!({"account": "other"}), 400, "other"),
        (serde_json::json!({"executors": ["vm"]}), 400, "vm"),
    ] {
        let r = patch(&auth, body.clone()).await;
        assert_eq!(r.status, status, "{body}: {}", r.text());
        let error = r.json()["error"].as_str().unwrap().to_string();
        assert!(error.contains(text), "{body}: {error}");
    }
    for who in [&lab, &viewer] {
        let r = patch(who, serde_json::json!({"network": "allow"})).await;
        assert_eq!(r.status, 403, "{}", r.text());
        let r = patch(who, serde_json::json!({})).await;
        assert_eq!(r.status, 403, "{}", r.text());
    }
    let after = request(addr, "GET", policy, std::slice::from_ref(&auth), b"")
        .await
        .json();
    assert_eq!(after, before);

    // A submitter reads its own account's policy, not another's; a
    // viewer reads any.
    let r = request(addr, "GET", policy, std::slice::from_ref(&lab), b"").await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json(), before);
    let r = request(
        addr,
        "GET",
        "/api/v1/accounts/other/policy",
        std::slice::from_ref(&lab),
        b"",
    )
    .await;
    assert_eq!(r.status, 403, "{}", r.text());
    let r = request(addr, "GET", policy, std::slice::from_ref(&viewer), b"").await;
    assert_eq!(r.status, 200, "{}", r.text());

    // whoami: a submitter sees what its jobs may ask for.
    let me = request(
        addr,
        "GET",
        "/api/v1/whoami",
        std::slice::from_ref(&lab),
        b"",
    )
    .await
    .json();
    assert_eq!(me["name"], "lab scripts");
    assert_eq!(me["account"], "lab");
    for field in [
        "images",
        "require_digest",
        "executors",
        "network",
        "selector",
        "tolerations",
        "max_timeout_ms",
        "max_gpus_per_task",
    ] {
        assert_eq!(me["policy"][field], before[field], "{field}");
    }
    assert_eq!(me["quota"], before["quota"]);
    assert_eq!(me["usage"]["running"], 0);
    let me = request(
        addr,
        "GET",
        "/api/v1/whoami",
        std::slice::from_ref(&auth),
        b"",
    )
    .await
    .json();
    assert_eq!(me["role"], "Admin");
    assert_eq!(me["policy"], serde_json::Value::Null);

    // validate=1: the answer a real submission would get, and no job.
    let boundary = "pumasi-test-boundary";
    let form = (
        "Content-Type",
        format!("multipart/form-data; boundary={boundary}"),
    );
    let submit = |path: &'static str, body: Vec<u8>| {
        let headers = [lab.clone(), form.clone()];
        async move { request(addr, "POST", path, &headers, &body).await }
    };
    let jobs = || async {
        request(
            addr,
            "GET",
            "/api/v1/status",
            std::slice::from_ref(&auth),
            b"",
        )
        .await
        .json()["jobs"]
            .as_array()
            .unwrap()
            .len()
    };
    let r = submit("/api/v1/jobs?validate=1", job_form(boundary, "checked")).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(v["valid"], true);
    assert_eq!(v["account"], "lab");
    assert_eq!(v["tasks"], 2);
    assert_eq!(v["ready_nodes"], serde_json::json!(["ui-node"]));
    assert_eq!(v["waiting"], serde_json::Value::Null);
    assert_eq!(jobs().await, 0);

    let docker = "name = \"refused\"\nimage = \"python:3.12-slim\"\ncommand = [\"true\"]\n";
    let checked = submit("/api/v1/jobs?validate=1", toml_form(boundary, docker)).await;
    assert_eq!(checked.status, 403, "{}", checked.text());
    let real = submit("/api/v1/jobs", toml_form(boundary, docker)).await;
    assert_eq!(real.status, 403, "{}", real.text());
    assert_eq!(checked.json(), real.json());
    assert!(
        real.json()["error"].as_str().unwrap().contains("docker"),
        "{}",
        real.text()
    );
    // A job that is wrong in itself is a 400 either way.
    let broken = "name = \"broken\"\nmodule = \"missing.wasm\"\n";
    let checked = submit("/api/v1/jobs?validate=1", toml_form(boundary, broken)).await;
    assert_eq!(checked.status, 400, "{}", checked.text());
    let real = submit("/api/v1/jobs", toml_form(boundary, broken)).await;
    assert_eq!(real.status, 400, "{}", real.text());
    for r in [&checked, &real] {
        assert!(r.text().contains("missing.wasm"), "{}", r.text());
    }
    let r = submit("/api/v1/jobs?validate=maybe", job_form(boundary, "x")).await;
    assert_eq!(r.status, 400, "{}", r.text());
    assert_eq!(jobs().await, 0);

    // A real run: each task says which node ran it and how long each
    // part took.
    let r = submit("/api/v1/jobs", job_form(boundary, "timed")).await;
    assert_eq!(r.status, 201, "{}", r.text());
    let id = r.json()["id"].as_str().unwrap().to_string();
    let mut job = serde_json::Value::Null;
    for _ in 0..300 {
        job = request(
            addr,
            "GET",
            &format!("/api/v1/jobs/{id}"),
            std::slice::from_ref(&lab),
            b"",
        )
        .await
        .json();
        if job["summary"]["state"] != "Running" && job["summary"]["state"] != "Queued" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(job["summary"]["state"], "Succeeded", "{job}");
    let tasks = job["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 2);
    for t in tasks {
        assert_eq!(t["node_name"], "ui-node", "{t}");
        assert!(t["node"].as_str().is_some_and(|n| n != "ui-node"), "{t}");
        assert_eq!(t["run_ms"], t["wall_ms"], "{t}");
        assert!(t["run_ms"].as_u64().unwrap() > 0, "{t}");
        assert_eq!(t["pull_ms"], 0, "{t}");
        for field in ["queued_ms", "fetch_ms", "upload_ms"] {
            assert!(t[field].is_u64(), "{field}: {t}");
        }
    }

    // The dashboard: the Accounts page and the job's times.
    let url = ui.url();
    let r = request(addr, "GET", &url[url.find("/login").unwrap()..], &[], b"").await;
    let cookie = r
        .header("set-cookie")
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let page = request(addr, "GET", "/accounts", &[("Cookie", cookie.clone())], b"").await;
    assert_eq!(page.status, 200, "{}", page.text());
    assert!(
        page.text().contains("data-account=\"lab\""),
        "{}",
        page.text()
    );
    let page = request(
        addr,
        "GET",
        "/accounts/lab",
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await;
    assert_eq!(page.status, 200, "{}", page.text());
    assert!(page.text().contains("node:22-slim"), "{}", page.text());
    let page = request(
        addr,
        "GET",
        &format!("/jobs/{id}/tasks/0"),
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await;
    assert_eq!(page.status, 200, "{}", page.text());
    for part in ["queued", "fetch", "pull", "run", "upload"] {
        assert!(
            page.text().contains(&format!("data-time=\"{part}\"")),
            "{part}: {}",
            page.text()
        );
    }
    assert!(page.text().contains("ui-node"), "{}", page.text());

    ui.stop();
    agent_task.abort();
}

/// A stand-in LAN card, and the magic packets asked of it.
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
                addr: [198, 18, 78, host],
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
    ) -> anyhow::Result<()> {
        self.sent.lock().unwrap().push(*mac);
        Ok(())
    }
}

/// Release 1.5: a node that said it was going to sleep shows as asleep,
/// its page offers Wake, and the API sends its magic packet through a
/// machine on its LAN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sleeping_node_shows_as_asleep_and_wakes_from_the_api() {
    use pumasi_agent::power::{FakePower, PowerEvent};
    let (cluster, agent_task) = start_cluster().await;
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let os = std::sync::Arc::new(FakePower::default());
    let lan = Lan::new(20);
    let desk_lan = Lan::new(21);
    let agent = |name: &str, power: pumasi_agent::PowerOptions| AgentConfig {
        data_dir: tempdir(name),
        name: name.into(),
        slots: Some(1),
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
        power,
        max_cpu_percent: None,
        availability: None,
        bench: None,
    };
    let laptop = Agent::new(agent(
        "laptop",
        pumasi_agent::PowerOptions {
            backend: Some(os.clone()),
            wake_on_lan: true,
            wake: Some(lan.clone()),
            ..Default::default()
        },
    ))
    .await
    .unwrap();
    let laptop_task = tokio::spawn(laptop.join(ticket.clone()));
    let desk = Agent::new(agent(
        "desk",
        pumasi_agent::PowerOptions {
            wake: Some(desk_lan.clone()),
            ..Default::default()
        },
    ))
    .await
    .unwrap();
    let desk_task = tokio::spawn(desk.join(ticket));
    for _ in 0..200 {
        if cluster.status().nodes.iter().filter(|n| n.online).count() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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
            public_url: None,
        },
    )
    .await
    .unwrap();
    let addr = ui.addr();
    let url = ui.url();
    let r = request(addr, "GET", &url[url.find("/login").unwrap()..], &[], b"").await;
    let cookie = r
        .header("set-cookie")
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let auth = ("Authorization", format!("Bearer {admin}"));
    let power_of = |name: &'static str| {
        let auth = auth.clone();
        async move {
            let s = request(addr, "GET", "/api/v1/status", &[auth], b"")
                .await
                .json();
            s["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["info"]["name"] == name)
                .unwrap_or_else(|| panic!("no node {name}: {s}"))
                .clone()
        }
    };

    // Awake: the report is in the API, and an online node is not woken.
    let mut node = power_of("laptop").await;
    for _ in 0..100 {
        if node["power"]["report"].is_object() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        node = power_of("laptop").await;
    }
    assert_eq!(node["power"]["report"]["wake_on_lan"], true, "{node}");
    assert_eq!(
        node["power"]["report"]["lan"][0]["mac"],
        serde_json::json!([2, 0, 0, 0, 0, 20]),
        "{node}"
    );
    assert!(node["power"]["asleep_since_unix"].is_null(), "{node}");
    let id = node["id"].as_str().unwrap().to_string();
    let r = request(
        addr,
        "POST",
        &format!("/api/v1/nodes/{id}/wake"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_ne!(r.status, 200, "{}", r.text());
    let page = request(
        addr,
        "GET",
        &format!("/nodes/{id}"),
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await
    .text();
    assert!(page.contains("02:00:00:00:00:14"), "{page}");
    assert!(!page.contains("data-action=\"wake\""), "{page}");

    // It goes to sleep.
    let os2 = os.clone();
    tokio::task::spawn_blocking(move || os2.announce(PowerEvent::Sleep))
        .await
        .unwrap();
    let mut node = power_of("laptop").await;
    for _ in 0..100 {
        if node["power"]["asleep_since_unix"].is_u64() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        node = power_of("laptop").await;
    }
    assert!(node["power"]["asleep_since_unix"].is_u64(), "{node}");
    assert_eq!(node["online"], false, "{node}");
    assert_eq!(node["power"]["can_wake"], true, "{node}");
    let list = request(addr, "GET", "/nodes", &[("Cookie", cookie.clone())], b"")
        .await
        .text();
    assert!(list.contains("data-state=\"asleep\""), "{list}");
    let page = request(
        addr,
        "GET",
        &format!("/nodes/{id}"),
        &[("Cookie", cookie.clone())],
        b"",
    )
    .await
    .text();
    assert!(page.contains("data-state=\"asleep\""), "{page}");
    assert!(page.contains("data-action=\"wake\""), "{page}");

    // Wake: a machine on its LAN sends the packet.
    let r = request(
        addr,
        "POST",
        &format!("/api/v1/nodes/{id}/wake"),
        std::slice::from_ref(&auth),
        b"",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.text().contains("magic packet"), "{}", r.text());
    for _ in 0..100 {
        if !desk_lan.sent.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(desk_lan.sent.lock().unwrap()[0], lan.iface.mac);

    os.announce(PowerEvent::Wake);
    ui.stop();
    agent_task.abort();
    laptop_task.abort();
    desk_task.abort();
}
