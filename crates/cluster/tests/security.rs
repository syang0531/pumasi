//! Self-hosting 1.0 (phase 17) over real iroh connections: the audit
//! log's hash chain (calls, refusals, restarts), replacing the admin
//! ticket, and signing in with OpenID Connect (browser and device flows)
//! and LDAP against stand-ins for a provider and a directory.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use axum::{
    Form, Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    audit::{AuditEntry, GENESIS, verify_chain},
    manage::AccessRole,
    msg::{AdminOp, AdminResponse},
    net::NetMode,
    sso::{Hidden, LdapSettings, OidcSettings, RoleMap, SsoUpdate, Subject},
    ticket::AdminTicket,
};
use serde_json::{Value, json};

fn tempdir(tag: &str) -> PathBuf {
    use std::hash::{BuildHasher, RandomState};
    let n = RandomState::new().hash_one(std::time::SystemTime::now());
    let p = std::env::temp_dir().join(format!("pumasi-security-{tag}-{n}"));
    std::fs::create_dir_all(&p).unwrap();
    p
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
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut config = ClusterConfig::new(dir);
    config.name = "security".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    Cluster::start(config).await
}

/// The answer, with errors (which the client returns as `Err`) as
/// `AdminResponse::Error`.
async fn call(c: &AdminClient, op: AdminOp) -> AdminResponse {
    match c.call(op).await {
        Ok(r) => r,
        Err(e) => AdminResponse::Error(format!("{e:#}")),
    }
}

async fn ok(c: &AdminClient, op: AdminOp) -> AdminResponse {
    match call(c, op).await {
        AdminResponse::Error(e) => panic!("call failed: {e}"),
        r => r,
    }
}

async fn err(c: &AdminClient, op: AdminOp) -> String {
    match call(c, op).await {
        AdminResponse::Error(e) => e,
        r => panic!("expected an error, got {r:?}"),
    }
}

async fn audit(c: &AdminClient) -> Vec<AuditEntry> {
    match ok(
        c,
        AdminOp::Audit {
            after: 0,
            limit: 10_000,
        },
    )
    .await
    {
        AdminResponse::Audit(v) => v,
        r => panic!("{r:?}"),
    }
}

async fn ticket_of(c: &AdminClient, name: &str, role: AccessRole) -> AdminTicket {
    match ok(
        c,
        AdminOp::CreateAccess {
            name: name.into(),
            role,
            ttl_secs: None,
        },
    )
    .await
    {
        AdminResponse::AccessTicket { ticket, .. } => ticket.parse().unwrap(),
        r => panic!("{r:?}"),
    }
}

/// A ticket with the cluster's address and no valid secret: what someone
/// who has not signed in yet holds.
fn anonymous(t: &AdminTicket) -> AdminTicket {
    AdminTicket {
        cluster: t.cluster.clone(),
        secret: [0; 32],
        replicas: t.replicas.clone(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_audit_log_chains_calls_refusals_and_restarts() -> Result<()> {
    init_tracing();
    let dir = tempdir("audit");
    let cluster = start_cluster(dir.clone()).await?;
    let admin = AdminClient::connect(&cluster.admin_ticket(), NetMode::Local).await?;

    ok(
        &admin,
        AdminOp::SecretSet {
            name: "registry/example.com".into(),
            value: "user:hunter2".into(),
        },
    )
    .await;
    let viewer = ticket_of(&admin, "carol", AccessRole::Viewer).await;
    let viewer = AdminClient::connect(&viewer, NetMode::Local).await?;
    let refused = err(&viewer, AdminOp::RemoveNode { id: "x".into() }).await;
    assert!(refused.contains("viewer"), "{refused}");
    // Reads are not logged.
    ok(&viewer, AdminOp::Status).await;
    // A wrong ticket is refused before anything is logged.
    let wrong = AdminClient::connect(&anonymous(&cluster.admin_ticket()), NetMode::Local).await?;
    assert!(
        err(&wrong, AdminOp::Status)
            .await
            .contains("invalid admin credentials")
    );

    let log = audit(&admin).await;
    let actions: Vec<(&str, &str, bool)> = log
        .iter()
        .map(|e| (e.action.as_str(), e.actor.as_str(), e.ok))
        .collect();
    assert_eq!(
        actions,
        [
            ("control-plane.start", "control plane", true),
            ("secret.set", "cluster admin", true),
            ("access.create", "cluster admin", true),
            ("node.remove", "carol", false),
        ]
    );
    assert!(log.iter().all(|e| !e.detail.contains("hunter2")));
    assert_eq!(log[3].role, "viewer");
    assert!(!log[3].peer.is_empty());
    verify_chain(GENESIS, 1, &log)?;
    // Viewers cannot read it.
    assert!(
        err(
            &viewer,
            AdminOp::Audit {
                after: 0,
                limit: 10
            }
        )
        .await
        .contains("audit")
    );

    let head = match ok(&admin, AdminOp::AuditVerify).await {
        AdminResponse::AuditHead(h) => h,
        r => panic!("{r:?}"),
    };
    head.verify()?;
    // The viewer's refused read is #5.
    let log = audit(&admin).await;
    assert_eq!((log[4].action.as_str(), log[4].ok), ("audit.read", false));
    assert_eq!(head.seq, 5);
    assert_eq!(head.hash, log[4].hash);
    assert_eq!(head.cluster, cluster.admin_ticket().cluster.id.to_string());

    // The chain goes on after a restart.
    admin.close().await;
    viewer.close().await;
    wrong.close().await;
    cluster.shutdown().await?;
    let cluster = start_cluster(dir.clone()).await?;
    let admin = AdminClient::connect(&cluster.admin_ticket(), NetMode::Local).await?;
    err(
        &admin,
        AdminOp::Cancel {
            id: pumasi_proto::job::JobId(7),
        },
    )
    .await;
    let log = audit(&admin).await;
    assert_eq!(log.len(), 7);
    assert_eq!(log[5].action, "control-plane.start");
    assert_eq!((log[6].action.as_str(), log[6].ok), ("job.cancel", false));
    verify_chain(GENESIS, 1, &log)?;
    // Paging: after #4, oldest first.
    match ok(&admin, AdminOp::Audit { after: 4, limit: 1 }).await {
        AdminResponse::Audit(v) => assert_eq!(v.iter().map(|e| e.seq).collect::<Vec<_>>(), [5]),
        r => panic!("{r:?}"),
    }
    match ok(&admin, AdminOp::AuditVerify).await {
        AdminResponse::AuditHead(h) => assert_eq!(h.seq, 7),
        r => panic!("{r:?}"),
    }
    admin.close().await;
    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotating_the_admin_secret_retires_the_old_ticket() -> Result<()> {
    init_tracing();
    let dir = tempdir("rotate");
    let cluster = start_cluster(dir.clone()).await?;
    let old = cluster.admin_ticket();
    let admin = AdminClient::connect(&old, NetMode::Local).await?;
    let new: AdminTicket = match ok(&admin, AdminOp::RotateAdminSecret).await {
        AdminResponse::AdminTicket { ticket } => ticket.parse()?,
        r => panic!("{r:?}"),
    };
    assert_ne!(new.secret, old.secret);
    assert!(
        err(&admin, AdminOp::Status)
            .await
            .contains("invalid admin credentials")
    );
    let fresh = AdminClient::connect(&new, NetMode::Local).await?;
    ok(&fresh, AdminOp::Status).await;
    // The file next to the cluster has the new one.
    let saved: AdminTicket =
        std::fs::read_to_string(dir.join(pumasi_cluster::ADMIN_TICKET_FILE))?.parse()?;
    assert_eq!(saved.secret, new.secret);
    let log = audit(&fresh).await;
    assert_eq!(log.last().unwrap().action, "admin.rotate");
    // And it survives a restart.
    admin.close().await;
    fresh.close().await;
    cluster.shutdown().await?;
    let cluster = start_cluster(dir.clone()).await?;
    assert_eq!(cluster.admin_ticket().secret, new.secret);
    cluster.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// A stand-in OpenID Connect provider
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Idp {
    issuer: String,
    /// ES256 key pair (PKCS#8) and its public point.
    pkcs8: Vec<u8>,
    point: Vec<u8>,
    /// Another key, for forged tokens.
    evil_pkcs8: Vec<u8>,
    /// Claims of the next token: sub, email, verified, groups.
    user: (String, String, bool, Vec<String>),
    /// Codes handed out: code -> (nonce, challenge, redirect_uri).
    codes: HashMap<String, (String, String, String)>,
    device_approved: bool,
    forge: bool,
    wrong_nonce: bool,
}

type SharedIdp = Arc<Mutex<Idp>>;

fn b64(b: &[u8]) -> String {
    data_encoding::BASE64URL_NOPAD.encode(b)
}

fn sign_token(idp: &Idp, nonce: Option<&str>) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (sub, email, verified, groups) = &idp.user;
    let mut claims = json!({
        "iss": idp.issuer, "aud": "pumasi-test", "sub": sub, "email": email,
        "email_verified": verified, "groups": groups, "iat": now, "exp": now + 300,
    });
    if let Some(n) = nonce {
        claims["nonce"] = json!(if idp.wrong_nonce { "nope" } else { n });
    }
    let key = if idp.forge {
        &idp.evil_pkcs8
    } else {
        &idp.pkcs8
    };
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    header.kid = Some("k1".into());
    jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_ec_der(key),
    )
    .unwrap()
}

async fn discovery(State(idp): State<SharedIdp>) -> Json<Value> {
    let i = idp.lock().unwrap().issuer.clone();
    Json(json!({
        "issuer": i,
        "authorization_endpoint": format!("{i}/authorize"),
        "token_endpoint": format!("{i}/token"),
        "jwks_uri": format!("{i}/jwks"),
        "device_authorization_endpoint": format!("{i}/device"),
    }))
}

async fn jwks(State(idp): State<SharedIdp>) -> Json<Value> {
    let i = idp.lock().unwrap();
    let (x, y) = i.point[1..].split_at(32);
    Json(
        json!({"keys": [{"kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig",
                          "kid": "k1", "x": b64(x), "y": b64(y)}]}),
    )
}

/// The user agrees at once: back to the redirect URI with a code.
async fn authorize(
    State(idp): State<SharedIdp>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    assert_eq!(q["response_type"], "code");
    assert_eq!(q["client_id"], "pumasi-test");
    assert_eq!(q["code_challenge_method"], "S256");
    assert!(q["scope"].split(' ').any(|s| s == "openid"));
    let code = format!("code-{}", rand::random::<u32>());
    idp.lock().unwrap().codes.insert(
        code.clone(),
        (
            q["nonce"].clone(),
            q["code_challenge"].clone(),
            q["redirect_uri"].clone(),
        ),
    );
    let to = format!("{}?code={code}&state={}", q["redirect_uri"], q["state"]);
    (StatusCode::FOUND, [(header::LOCATION, to)]).into_response()
}

async fn token(State(idp): State<SharedIdp>, Form(f): Form<HashMap<String, String>>) -> Response {
    let mut i = idp.lock().unwrap();
    if f.get("client_secret").map(String::as_str) != Some("s3cret") {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid_client"})),
        )
            .into_response();
    }
    match f["grant_type"].as_str() {
        "authorization_code" => {
            let Some((nonce, challenge, redirect)) = i.codes.remove(&f["code"]) else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid_grant"})),
                )
                    .into_response();
            };
            use sha2::Digest;
            let want = b64(&sha2::Sha256::digest(f["code_verifier"].as_bytes()));
            if want != challenge || f["redirect_uri"] != redirect {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid_grant"})),
                )
                    .into_response();
            }
            Json(json!({"id_token": sign_token(&i, Some(&nonce)), "token_type": "Bearer"}))
                .into_response()
        }
        "urn:ietf:params:oauth:grant-type:device_code" => {
            assert_eq!(f["device_code"], "dev-1");
            if !i.device_approved {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "authorization_pending"})),
                )
                    .into_response();
            }
            i.device_approved = false;
            Json(json!({"id_token": sign_token(&i, None)})).into_response()
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "unsupported_grant_type"})),
        )
            .into_response(),
    }
}

async fn device(Form(f): Form<HashMap<String, String>>) -> Json<Value> {
    assert_eq!(f["client_id"], "pumasi-test");
    Json(json!({"device_code": "dev-1", "user_code": "ABCD-EFGH",
                "verification_uri": "https://idp.example/device", "interval": 1, "expires_in": 60}))
}

async fn start_idp() -> (SharedIdp, SocketAddr) {
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let pair =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let evil = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let idp = Arc::new(Mutex::new(Idp {
        issuer: format!("http://{addr}"),
        pkcs8: pkcs8.as_ref().to_vec(),
        point: pair.public_key().as_ref().to_vec(),
        evil_pkcs8: evil.as_ref().to_vec(),
        user: (
            "u-1".into(),
            "Alice@Corp.example".into(),
            true,
            vec!["ml-team".into()],
        ),
        ..Default::default()
    }));
    let app = Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/authorize", get(authorize))
        .route("/token", post(token))
        .route("/device", post(device))
        .with_state(idp.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (idp, addr)
}

/// What a browser does: start, follow the provider's redirect, finish.
async fn browser_sign_in(anon: &AdminClient, http: &reqwest::Client) -> AdminResponse {
    let redirect_uri = "http://127.0.0.1:7400/sso/callback";
    let (url, state) = match ok(
        anon,
        AdminOp::SsoStart {
            redirect_uri: redirect_uri.into(),
        },
    )
    .await
    {
        AdminResponse::SsoRedirect { url, state } => (url, state),
        r => panic!("{r:?}"),
    };
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 302);
    let back = resp.headers()["location"].to_str().unwrap().to_string();
    let q: HashMap<String, String> = back
        .split_once('?')
        .unwrap()
        .1
        .split('&')
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap();
            (k.to_string(), v.to_string())
        })
        .collect();
    assert!(back.starts_with(redirect_uri));
    assert_eq!(q["state"], state);
    call(
        anon,
        AdminOp::SsoFinish {
            state,
            code: q["code"].clone(),
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oidc_sign_in_maps_groups_to_roles() -> Result<()> {
    init_tracing();
    let (idp, idp_addr) = start_idp().await;
    let cluster = start_cluster(tempdir("oidc")).await?;
    let admin = AdminClient::connect(&cluster.admin_ticket(), NetMode::Local).await?;
    let anon = AdminClient::connect(&anonymous(&cluster.admin_ticket()), NetMode::Local).await?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    // Nothing set up yet.
    match ok(&anon, AdminOp::SsoInfo).await {
        AdminResponse::SsoInfo(i) => assert_eq!(i, Default::default()),
        r => panic!("{r:?}"),
    }
    assert!(
        err(&anon, AdminOp::SsoDeviceStart)
            .await
            .contains("no OpenID Connect")
    );
    // Sign-in calls are the only ones without a ticket.
    assert!(
        err(&anon, AdminOp::SsoGet)
            .await
            .contains("invalid admin credentials")
    );

    let settings = OidcSettings {
        name: "Corp".into(),
        issuer: format!("http://{idp_addr}"),
        client_id: "pumasi-test".into(),
        client_secret: Hidden("s3cret".into()),
        scopes: vec!["email".into(), "profile".into()],
        groups_claim: "groups".into(),
    };
    ok(
        &admin,
        AdminOp::SsoSet(Box::new(SsoUpdate::Oidc(Some(Box::new(settings))))),
    )
    .await;
    let view = match ok(&admin, AdminOp::SsoGet).await {
        AdminResponse::Sso(v) => v,
        r => panic!("{r:?}"),
    };
    assert!(view.oidc.as_ref().unwrap().has_secret);
    assert!(!format!("{view:?}").contains("s3cret"));
    match ok(&anon, AdminOp::SsoInfo).await {
        AdminResponse::SsoInfo(i) => {
            assert_eq!(i.oidc.as_deref(), Some("Corp"));
            // The device flow is off until an admin turns it on.
            assert!(!i.device && !i.ldap);
        }
        r => panic!("{r:?}"),
    }
    // A bad redirect URI is refused before anything happens.
    assert!(
        err(
            &anon,
            AdminOp::SsoStart {
                redirect_uri: "http://evil.example/sso/callback".into()
            }
        )
        .await
        .contains("redirect URI")
    );

    // No rule yet: refused, and logged.
    match browser_sign_in(&anon, &http).await {
        AdminResponse::Error(e) => assert!(e.contains("no rule"), "{e}"),
        r => panic!("{r:?}"),
    }
    let map = |subject: &str, role: AccessRole| {
        AdminOp::SsoSet(Box::new(SsoUpdate::Map(RoleMap {
            subject: subject.parse::<Subject>().unwrap(),
            role,
        })))
    };
    ok(
        &admin,
        map(
            "group:ml-team",
            AccessRole::Submitter {
                account: "ml".into(),
            },
        ),
    )
    .await;
    ok(&admin, map("domain:corp.example", AccessRole::Viewer)).await;

    let (ticket, view) = match browser_sign_in(&anon, &http).await {
        AdminResponse::AccessTicket { ticket, view } => (ticket.parse::<AdminTicket>()?, view),
        r => panic!("{r:?}"),
    };
    assert_eq!(view.name, "sso:alice@corp.example");
    assert_eq!(
        view.role,
        AccessRole::Submitter {
            account: "ml".into()
        }
    );
    let expires = view.expires_unix.unwrap() - view.created_unix;
    assert_eq!(expires, 12 * 3600);
    let alice = AdminClient::connect(&ticket, NetMode::Local).await?;
    match ok(&alice, AdminOp::WhoAmI).await {
        AdminResponse::WhoAmI { name, role } => {
            assert_eq!(name, "sso:alice@corp.example");
            assert_eq!(
                role,
                AccessRole::Submitter {
                    account: "ml".into()
                }
            );
        }
        r => panic!("{r:?}"),
    }
    // A code cannot be used twice, and a state only once.
    assert!(
        err(
            &anon,
            AdminOp::SsoFinish {
                state: "made-up".into(),
                code: "x".into()
            }
        )
        .await
        .contains("unknown or too old")
    );

    // Unverified email: the domain rule does not apply.
    idp.lock().unwrap().user = ("u-2".into(), "bob@corp.example".into(), false, vec![]);
    match browser_sign_in(&anon, &http).await {
        AdminResponse::Error(e) => assert!(e.contains("sso:u-2") && e.contains("no rule"), "{e}"),
        r => panic!("{r:?}"),
    }
    // Forged signature and replayed nonce are refused.
    idp.lock().unwrap().user = ("u-3".into(), "eve@corp.example".into(), true, vec![]);
    idp.lock().unwrap().forge = true;
    match browser_sign_in(&anon, &http).await {
        AdminResponse::Error(e) => assert!(e.contains("does not check out"), "{e}"),
        r => panic!("{r:?}"),
    }
    idp.lock().unwrap().forge = false;
    idp.lock().unwrap().wrong_nonce = true;
    match browser_sign_in(&anon, &http).await {
        AdminResponse::Error(e) => assert!(e.contains("nonce"), "{e}"),
        r => panic!("{r:?}"),
    }
    idp.lock().unwrap().wrong_nonce = false;

    // Device flow (pumasi login --sso): off by default, since anyone can
    // start one and talk a member into entering its code.
    assert!(
        err(&anon, AdminOp::SsoDeviceStart)
            .await
            .contains("device flow) is off")
    );
    ok(&admin, AdminOp::SsoSet(Box::new(SsoUpdate::Device(true)))).await;
    match ok(&anon, AdminOp::SsoInfo).await {
        AdminResponse::SsoInfo(i) => assert!(i.device),
        r => panic!("{r:?}"),
    }
    let start = match ok(&anon, AdminOp::SsoDeviceStart).await {
        AdminResponse::SsoDevice(d) => d,
        r => panic!("{r:?}"),
    };
    assert_eq!(start.user_code, "ABCD-EFGH");
    match ok(
        &anon,
        AdminOp::SsoDevicePoll {
            handle: start.handle.clone(),
        },
    )
    .await
    {
        AdminResponse::SsoPending { interval_secs } => assert!(interval_secs >= 1),
        r => panic!("{r:?}"),
    }
    idp.lock().unwrap().device_approved = true;
    let got = loop {
        tokio::time::sleep(Duration::from_millis(1100)).await;
        match ok(
            &anon,
            AdminOp::SsoDevicePoll {
                handle: start.handle.clone(),
            },
        )
        .await
        {
            AdminResponse::SsoPending { .. } => continue,
            AdminResponse::AccessTicket { view, .. } => break view,
            r => panic!("{r:?}"),
        }
    };
    assert_eq!(got.name, "sso:eve@corp.example");
    assert_eq!(got.role, AccessRole::Viewer);
    assert!(
        err(
            &anon,
            AdminOp::SsoDevicePoll {
                handle: start.handle
            }
        )
        .await
        .contains("unknown")
    );

    // Revoking by name ends every session of that person.
    match ok(
        &admin,
        AdminOp::RevokeAccess {
            id: "sso:alice@corp.example".into(),
        },
    )
    .await
    {
        AdminResponse::AccessList(v) => assert_eq!(v.len(), 1),
        r => panic!("{r:?}"),
    }
    assert!(
        err(&alice, AdminOp::WhoAmI)
            .await
            .contains("invalid admin credentials")
    );

    let log = audit(&admin).await;
    let logins: Vec<(&str, &str, bool)> = log
        .iter()
        .filter(|e| e.action == "sso.login")
        .map(|e| (e.target.as_str(), e.role.as_str(), e.ok))
        .collect();
    assert_eq!(
        logins,
        [
            ("sso:alice@corp.example", "anonymous", false),
            ("sso:alice@corp.example", "submitter (ml)", true),
            ("sso:u-2", "anonymous", false),
            ("sso:eve@corp.example", "viewer", true),
        ]
    );
    verify_chain(GENESIS, 1, &log)?;
    for c in [admin, anon, alice] {
        c.close().await;
    }
    cluster.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// A stand-in LDAP directory
// ---------------------------------------------------------------------------

/// `uid=alice` (password `wonderland`, in `cn=ops,ou=groups`) and `uid=bob`
/// (password `builder`, in a `cn=ops` among contractors) under `ou=people,dc=example,dc=com`;
/// the service account is `cn=svc,dc=example,dc=com` / `svcpw`.
async fn start_ldap() -> SocketAddr {
    use futures::{SinkExt, StreamExt};
    use ldap3_proto::{LdapCodec, simple::*};
    use tokio_util::codec::{FramedRead, FramedWrite};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let users: HashMap<&str, (&str, Vec<&str>)> = HashMap::from([
        (
            "alice",
            ("wonderland", vec!["cn=ops,ou=groups,dc=example,dc=com"]),
        ),
        // A group of the same name elsewhere in the directory.
        (
            "bob",
            ("builder", vec!["cn=ops,ou=contractors,dc=example,dc=com"]),
        ),
    ]);
    let users = Arc::new(users);
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let users = users.clone();
            tokio::spawn(async move {
                let (r, w) = tokio::io::split(sock);
                let mut reqs = FramedRead::new(r, LdapCodec::default());
                let mut resp = FramedWrite::new(w, LdapCodec::default());
                let mut bound = false;
                while let Some(Ok(msg)) = reqs.next().await {
                    let Ok(op) = ServerOps::try_from(msg) else {
                        return;
                    };
                    let out = match op {
                        ServerOps::SimpleBind(b) => {
                            let ok = if b.dn == "cn=svc,dc=example,dc=com" {
                                b.pw == "svcpw"
                            } else if let Some(uid) =
                                b.dn.strip_prefix("uid=")
                                    .and_then(|r| r.strip_suffix(",ou=people,dc=example,dc=com"))
                            {
                                users.get(uid).is_some_and(|(pw, _)| *pw == b.pw)
                            } else {
                                // Like a real server: an empty password
                                // is an unauthenticated bind.
                                b.pw.is_empty()
                            };
                            bound = ok;
                            vec![if ok {
                                b.gen_success()
                            } else {
                                b.gen_invalid_cred()
                            }]
                        }
                        ServerOps::Search(s) => {
                            if !bound {
                                vec![s.gen_error(
                                    LdapResultCode::InsufficentAccessRights,
                                    "bind first".into(),
                                )]
                            } else {
                                // The filter is (uid=<escaped>): pick the value.
                                let f = format!("{:?}", s.filter);
                                let mut out: Vec<LdapMsg> = users
                                    .iter()
                                    .filter(|(uid, _)| f.contains(&format!("\"{uid}\"")))
                                    .map(|(uid, (_, groups))| {
                                        s.gen_result_entry(LdapSearchResultEntry {
                                            dn: format!("uid={uid},ou=people,dc=example,dc=com"),
                                            attributes: vec![
                                                LdapPartialAttribute {
                                                    atype: "memberOf".into(),
                                                    vals: groups
                                                        .iter()
                                                        .map(|g| g.as_bytes().to_vec())
                                                        .collect(),
                                                },
                                                LdapPartialAttribute {
                                                    atype: "mail".into(),
                                                    vals: vec![
                                                        format!("{uid}@example.com").into_bytes(),
                                                    ],
                                                },
                                            ],
                                        })
                                    })
                                    .collect();
                                out.push(s.gen_success());
                                out
                            }
                        }
                        ServerOps::Unbind(_) => return,
                        _ => continue,
                    };
                    for m in out {
                        if resp.send(m).await.is_err() {
                            return;
                        }
                    }
                    let _ = resp.flush().await;
                }
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ldap_sign_in_checks_passwords_and_groups() -> Result<()> {
    init_tracing();
    let dir_addr = start_ldap().await;
    let cluster = start_cluster(tempdir("ldap")).await?;
    let admin = AdminClient::connect(&cluster.admin_ticket(), NetMode::Local).await?;
    let anon = AdminClient::connect(&anonymous(&cluster.admin_ticket()), NetMode::Local).await?;
    let login = |user: &str, pw: &str| AdminOp::LdapLogin {
        user: user.into(),
        password: Hidden(pw.into()),
    };
    assert!(
        err(&anon, login("alice", "wonderland"))
            .await
            .contains("no LDAP")
    );

    let mut settings = LdapSettings {
        url: "ldap://directory.example.com".into(),
        starttls: false,
        bind_dn: "cn=svc,dc=example,dc=com".into(),
        bind_password: Hidden("svcpw".into()),
        user_base: "ou=people,dc=example,dc=com".into(),
        user_filter: "(uid={user})".into(),
        group_attr: "memberOf".into(),
        group_base: String::new(),
        group_filter: String::new(),
    };
    // Plain LDAP to another machine would send passwords in the clear.
    assert!(
        err(
            &admin,
            AdminOp::SsoSet(Box::new(SsoUpdate::Ldap(Some(Box::new(settings.clone())))))
        )
        .await
        .contains("ldaps")
    );
    settings.url = format!("ldap://{dir_addr}");
    ok(
        &admin,
        AdminOp::SsoSet(Box::new(SsoUpdate::Ldap(Some(Box::new(settings))))),
    )
    .await;
    // Without a group base, LDAP groups match by their whole DN only: a
    // bare `group:ops` must not match bob's `cn=ops` among contractors.
    for subject in ["group:cn=ops,ou=groups,dc=example,dc=com", "group:ops"] {
        ok(
            &admin,
            AdminOp::SsoSet(Box::new(SsoUpdate::Map(RoleMap {
                subject: subject.parse()?,
                role: AccessRole::Admin,
            }))),
        )
        .await;
    }

    match ok(&anon, login("alice", "wonderland")).await {
        AdminResponse::AccessTicket { view, .. } => {
            assert_eq!(view.name, "ldap:alice");
            assert_eq!(view.role, AccessRole::Admin);
        }
        r => panic!("{r:?}"),
    }
    // Wrong password, empty password, filter injection, unknown user.
    assert!(
        err(&anon, login("alice", "nope"))
            .await
            .contains("wrong user name or password")
    );
    assert!(
        err(&anon, login("alice", ""))
            .await
            .contains("wrong user name or password")
    );
    assert!(
        err(&anon, login("*", "x"))
            .await
            .contains("wrong user name or password")
    );
    assert!(
        err(&anon, login("mallory", "x"))
            .await
            .contains("wrong user name or password")
    );
    // Bob is in no group that has a rule (his `cn=ops` is not the one).
    assert!(
        err(&anon, login("bob", "builder"))
            .await
            .contains("no rule")
    );
    // Too many wrong passwords pause the user name, however it is
    // capitalized (directories ignore case, so the count does too).
    for user in ["bob", "Bob", "bob", "BOB", "bob"] {
        err(&anon, login(user, "guess")).await;
    }
    let paused = err(&anon, login("bob", "builder")).await;
    assert!(paused.contains("too many wrong passwords"), "{paused}");

    let log = audit(&admin).await;
    let ldap: Vec<(&str, bool)> = log
        .iter()
        .filter(|e| e.action == "ldap.login")
        .map(|e| (e.target.as_str(), e.ok))
        .collect();
    // Unknown users and injected names are not logged; known ones are.
    assert_eq!(ldap[0], ("ldap:alice", true));
    assert_eq!(ldap[1], ("ldap:alice", false));
    assert!(
        ldap.iter()
            .all(|(t, _)| *t == "ldap:alice" || *t == "ldap:bob")
    );
    // "no rule", then the three wrong passwords for the name the
    // directory knows (this test directory is case-sensitive).
    assert_eq!(ldap.iter().filter(|(t, _)| *t == "ldap:bob").count(), 4);
    admin.close().await;
    anon.close().await;
    cluster.shutdown().await?;
    Ok(())
}
