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
