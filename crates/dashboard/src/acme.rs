//! Certificates from an ACME server for `pumasi ui --acme DOMAIN` (1.19):
//! Let's Encrypt by default, or any RFC 8555 server (`--acme-directory`).
//!
//! The dashboard proves it holds the name with TLS-ALPN-01 on its own
//! HTTPS port (the CA connects to port 443 of the name, so the dashboard
//! listens there or behind a port forward), or with HTTP-01 on a small
//! plain HTTP listener (port 80) that answers challenge requests and
//! sends everything else to HTTPS.
//!
//! The account key, the certificate and its key are kept in the home
//! (`acme/`), so a restart does not order again. The certificate is
//! renewed 30 days before it expires and swapped in without a restart;
//! connections already open keep the old one.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    extract::{Path as UrlPath, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use data_encoding::BASE64URL_NOPAD;
use ring::signature::KeyPair;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject},
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use tokio_rustls_acme::acme::{
    ACME_TLS_ALPN_NAME, Account, Auth, AuthStatus, ChallengeType, Directory, Identifier, Order,
    OrderStatus,
};

pub use tokio_rustls_acme::acme::{
    LETS_ENCRYPT_PRODUCTION_DIRECTORY as LETS_ENCRYPT, LETS_ENCRYPT_STAGING_DIRECTORY as STAGING,
};

/// Renew this long before the certificate expires.
const RENEW_BEFORE: Duration = Duration::from_secs(30 * 24 * 3600);
/// How often to look at the certificate's age.
const CHECK_EVERY: Duration = Duration::from_secs(12 * 3600);
/// A failed order is tried again after this, doubling up to an hour.
const RETRY_FIRST: Duration = Duration::from_secs(60);
const RETRY_MAX: Duration = Duration::from_secs(3600);
/// One order, from new order to certificate, at most this long.
const ORDER_TIMEOUT: Duration = Duration::from_secs(300);

/// How the dashboard proves it holds the names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Challenge {
    /// TLS-ALPN-01, on the dashboard's own HTTPS port.
    TlsAlpn,
    /// HTTP-01, on a plain HTTP listener of its own.
    Http,
}

impl std::str::FromStr for Challenge {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "tls-alpn-01" | "tls-alpn" | "tls" => Ok(Self::TlsAlpn),
            "http-01" | "http" => Ok(Self::Http),
            _ => bail!("unknown ACME challenge {s:?}: use tls-alpn-01 or http-01"),
        }
    }
}

pub struct AcmeConfig {
    /// The names on the certificate; the first is the main one.
    pub domains: Vec<String>,
    /// A contact address the CA mails about expiring certificates.
    pub email: Option<String>,
    /// The ACME directory URL.
    pub directory: String,
    /// Extra roots (PEM) to trust for the ACME server itself, such as a
    /// test server's own CA.
    pub ca: Option<PathBuf>,
    /// Where the account key and certificates are kept.
    pub cache: PathBuf,
    pub challenge: Challenge,
    /// Where the HTTP-01 listener binds (with [`Challenge::Http`]).
    pub http_listen: SocketAddr,
}

/// Serves the current certificate, and the challenge certificates to a
/// CA that asks for `acme-tls/1`.
#[derive(Debug, Default)]
struct Resolver {
    cert: RwLock<Option<Arc<CertifiedKey>>>,
    alpn: Mutex<HashMap<String, Arc<CertifiedKey>>>,
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let challenge = hello
            .alpn()
            .is_some_and(|mut p| p.next() == Some(ACME_TLS_ALPN_NAME) && p.next().is_none());
        if challenge {
            let name = hello.server_name()?.to_ascii_lowercase();
            return self.alpn.lock().unwrap().get(&name).cloned();
        }
        self.cert.read().unwrap().clone()
    }
}

/// Keeps a certificate for the dashboard's names.
pub struct Acme {
    config: AcmeConfig,
    resolver: Arc<Resolver>,
    client: Arc<rustls::ClientConfig>,
    server: Arc<rustls::ServerConfig>,
    /// HTTP-01 key authorizations by token.
    tokens: Mutex<HashMap<String, String>>,
    /// When the current certificate expires.
    not_after: Mutex<Option<SystemTime>>,
}

impl Acme {
    /// Loads the kept certificate, if there is one for these names.
    pub fn new(mut config: AcmeConfig) -> Result<Arc<Self>> {
        anyhow::ensure!(!config.domains.is_empty(), "--acme needs a domain name");
        for d in &mut config.domains {
            *d = d.trim().trim_end_matches('.').to_ascii_lowercase();
            anyhow::ensure!(
                !d.is_empty()
                    && !d.starts_with('-')
                    && d.contains('.')
                    && d.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '*')
                    && d.parse::<std::net::IpAddr>().is_err(),
                "--acme {d:?}: give a DNS name such as pumasi.example.com"
            );
        }
        let mut roots = pumasi_proto::registry::root_certs();
        if let Some(ca) = &config.ca {
            for cert in CertificateDer::pem_file_iter(ca)
                .with_context(|| format!("read {}", ca.display()))?
            {
                roots
                    .add(cert.with_context(|| format!("parse {}", ca.display()))?)
                    .with_context(|| format!("add the CA in {}", ca.display()))?;
            }
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        let resolver = Arc::new(Resolver::default());
        let mut server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_cert_resolver(resolver.clone());
        // Browsers get HTTP/1.1; the CA, asking only for acme-tls/1, gets
        // that and the challenge certificate.
        server.alpn_protocols = vec![b"http/1.1".to_vec(), ACME_TLS_ALPN_NAME.to_vec()];
        let acme = Arc::new(Self {
            config,
            resolver,
            client: Arc::new(client),
            server: Arc::new(server),
            tokens: Mutex::default(),
            not_after: Mutex::default(),
        });
        if let Err(e) = acme.load() {
            tracing::warn!("the kept ACME certificate cannot be used: {e:#}");
        }
        Ok(acme)
    }

    /// The TLS configuration for the dashboard ([`crate::UiConfig::tls`]).
    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        self.server.clone()
    }

    /// When the certificate in use expires, if there is one.
    pub fn not_after(&self) -> Option<SystemTime> {
        *self.not_after.lock().unwrap()
    }

    /// The main name, for the URL to print.
    pub fn domain(&self) -> &str {
        &self.config.domains[0]
    }

    /// Orders a certificate when there is none or it expires within 30
    /// days, and keeps doing so. Runs until dropped; start it once the
    /// dashboard listens (TLS-ALPN-01 goes through its port).
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let _http = match self.config.challenge {
            Challenge::Http => Some(self.clone().serve_http().await?),
            Challenge::TlsAlpn => None,
        };
        let mut retry = RETRY_FIRST;
        loop {
            let due = match self.not_after() {
                None => Duration::ZERO,
                Some(t) => t
                    .checked_sub(RENEW_BEFORE)
                    .and_then(|at| at.duration_since(SystemTime::now()).ok())
                    .unwrap_or(Duration::ZERO),
            };
            if !due.is_zero() {
                tokio::time::sleep(due.min(CHECK_EVERY)).await;
                continue;
            }
            match self.renew().await {
                Ok(()) => {
                    retry = RETRY_FIRST;
                    tracing::info!(
                        domains = ?self.config.domains,
                        "ACME certificate in use until {}",
                        self.not_after().map(fmt_time).unwrap_or_default()
                    );
                }
                Err(e) => {
                    let have = self.not_after().is_some();
                    tracing::warn!("ACME order for {:?} failed: {e:#}", self.config.domains);
                    if !have {
                        eprintln!(
                            "warning: no certificate from {} yet: {e:#} (trying again in {}s)",
                            self.config.directory,
                            retry.as_secs()
                        );
                    }
                    tokio::time::sleep(retry).await;
                    retry = (retry * 2).min(RETRY_MAX);
                }
            }
        }
    }

    /// Orders a certificate now and puts it in use.
    pub async fn renew(&self) -> Result<()> {
        let (chain, key) = tokio::time::timeout(ORDER_TIMEOUT, self.order())
            .await
            .map_err(|_| anyhow!("the order took more than {}s", ORDER_TIMEOUT.as_secs()))??;
        let dir = self.cert_dir();
        std::fs::create_dir_all(&dir)?;
        pumasi_proto::net::write_private(&dir.join("key.pem"), key.as_bytes())?;
        pumasi_proto::net::write_private(&dir.join("cert.pem"), chain.as_bytes())?;
        self.use_pem(&chain, &key)
    }

    /// The kept certificate, if any; true when one is now in use.
    fn load(&self) -> Result<bool> {
        let dir = self.cert_dir();
        let (Ok(chain), Ok(key)) = (
            std::fs::read_to_string(dir.join("cert.pem")),
            std::fs::read_to_string(dir.join("key.pem")),
        ) else {
            return Ok(false);
        };
        self.use_pem(&chain, &key)?;
        Ok(true)
    }

    fn use_pem(&self, chain: &str, key: &str) -> Result<()> {
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain.as_bytes())
            .collect::<Result<_, _>>()
            .context("parse the certificate chain")?;
        let first = certs.first().context("no certificate in the chain")?;
        let not_after = not_after(first)?;
        anyhow::ensure!(not_after > SystemTime::now(), "the certificate has expired");
        let key = PrivateKeyDer::from_pem_slice(key.as_bytes()).context("parse the key")?;
        let key = rustls::crypto::ring::sign::any_supported_type(&key)
            .map_err(|e| anyhow!("unusable key: {e}"))?;
        *self.resolver.cert.write().unwrap() = Some(Arc::new(CertifiedKey::new(certs, key)));
        *self.not_after.lock().unwrap() = Some(not_after);
        Ok(())
    }

    /// Kept per directory and set of names, so changing either orders
    /// anew instead of serving a certificate for other names.
    fn cert_dir(&self) -> PathBuf {
        let mut names = self.config.domains.clone();
        names.sort();
        let id =
            blake3::hash(format!("{}\n{}", self.config.directory, names.join("\n")).as_bytes());
        self.account_dir().join(&id.to_hex()[..16])
    }

    fn account_dir(&self) -> PathBuf {
        let id = blake3::hash(self.config.directory.as_bytes());
        self.config.cache.join(&id.to_hex()[..16])
    }

    /// The account key for this directory, made the first time.
    fn account_key(&self) -> Result<Vec<u8>> {
        let path = self.account_dir().join("account.key");
        if let Ok(pem) = std::fs::read(&path) {
            let key = PrivatePkcs8KeyDer::from_pem_slice(&pem)
                .with_context(|| format!("parse {}", path.display()))?;
            return Ok(key.secret_pkcs8_der().to_vec());
        }
        let key = Account::generate_key_pair();
        std::fs::create_dir_all(self.account_dir())?;
        let pem = pem_encode("PRIVATE KEY", &key);
        pumasi_proto::net::write_private(&path, pem.as_bytes())?;
        Ok(key)
    }

    /// One order: returns the chain and its key, both PEM.
    async fn order(&self) -> Result<(String, String)> {
        let cc = &self.client;
        let directory = retry_nonce(|| Directory::discover(cc, &self.config.directory))
            .await
            .with_context(|| format!("ACME directory {}", self.config.directory))?;
        let key = self.account_key()?;
        let contact: Vec<String> = self
            .config
            .email
            .iter()
            .map(|e| format!("mailto:{e}"))
            .collect();
        let account = retry_nonce(|| {
            Account::create_with_keypair(cc, directory.clone(), &contact, &key, &None)
        })
        .await
        .context("ACME account")?;
        let thumbprint = jwk_thumbprint(&account);
        let (url, mut order) = retry_nonce(|| account.new_order(cc, self.config.domains.clone()))
            .await
            .context("new ACME order")?;
        let mut asked: Vec<String> = Vec::new();
        let mut key_pem: Option<String> = None;
        let mut wait = Duration::from_millis(250);
        let result = loop {
            match order.status.clone() {
                OrderStatus::Pending => {
                    for auth_url in &order.authorizations {
                        if asked.contains(auth_url) {
                            continue;
                        }
                        let auth = retry_nonce(|| account.auth(cc, auth_url)).await?;
                        match auth.status {
                            AuthStatus::Valid => {}
                            AuthStatus::Pending => {
                                let challenge_url = self.prepare(&account, &auth, &thumbprint)?;
                                retry_nonce(|| account.challenge(cc, &challenge_url)).await?;
                            }
                            _ => break_invalid(&auth)?,
                        }
                        asked.push(auth_url.clone());
                    }
                }
                OrderStatus::Ready => {
                    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
                    let mut params = rcgen::CertificateParams::new(self.config.domains.clone())?;
                    params.distinguished_name = rcgen::DistinguishedName::new();
                    let csr = params.serialize_request(&key)?;
                    let csr = csr.der().to_vec();
                    order = retry_nonce(|| account.finalize(cc, &order.finalize, csr.clone()))
                        .await
                        .context("finalize the ACME order")?;
                    key_pem = Some(key.serialize_pem());
                    continue;
                }
                OrderStatus::Processing => {}
                OrderStatus::Valid { certificate } => {
                    let chain = retry_nonce(|| account.certificate(cc, &certificate))
                        .await
                        .context("download the certificate")?;
                    let key = key_pem.take().context("the order was already finished")?;
                    break Ok((chain, key));
                }
                OrderStatus::Invalid => {
                    break Err(self.why_invalid(&account, &order).await);
                }
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(3));
            order = retry_nonce(|| account.order(cc, &url)).await?;
        };
        self.alpn_clear();
        result
    }

    /// Sets up the answer to one authorization; returns the challenge URL.
    fn prepare(&self, account: &Account, auth: &Auth, thumbprint: &str) -> Result<String> {
        let Identifier::Dns(name) = &auth.identifier;
        match self.config.challenge {
            Challenge::TlsAlpn => {
                let (challenge, cert) = account
                    .tls_alpn_01(&auth.challenges, name.clone())
                    .map_err(|e| anyhow!("{name}: {e}"))?;
                self.resolver
                    .alpn
                    .lock()
                    .unwrap()
                    .insert(name.to_ascii_lowercase(), Arc::new(cert));
                Ok(challenge.url.clone())
            }
            Challenge::Http => {
                let challenge = auth
                    .challenges
                    .iter()
                    .find(|c| c.typ == ChallengeType::Http01)
                    .with_context(|| format!("{name}: the CA offers no http-01 challenge"))?;
                self.tokens.lock().unwrap().insert(
                    challenge.token.clone(),
                    format!("{}.{thumbprint}", challenge.token),
                );
                Ok(challenge.url.clone())
            }
        }
    }

    fn alpn_clear(&self) {
        self.resolver.alpn.lock().unwrap().clear();
        self.tokens.lock().unwrap().clear();
    }

    /// Why an order failed, from its authorizations.
    async fn why_invalid(&self, account: &Account, order: &Order) -> anyhow::Error {
        for url in &order.authorizations {
            if let Ok(auth) = account.auth(&self.client, url).await
                && let Err(e) = break_invalid(&auth)
            {
                return e;
            }
        }
        match &order.error {
            Some(p) => anyhow!(
                "the CA refused the order: {}",
                p.detail.clone().unwrap_or_default()
            ),
            None => anyhow!("the CA refused the order"),
        }
    }

    /// The HTTP-01 listener: answers challenges and sends the rest to
    /// HTTPS.
    async fn serve_http(self: Arc<Self>) -> Result<crate::AbortOnDrop> {
        let listener = tokio::net::TcpListener::bind(self.config.http_listen)
            .await
            .with_context(|| format!("listen on {} for ACME http-01", self.config.http_listen))?;
        let app = axum::Router::new()
            .route("/.well-known/acme-challenge/{token}", get(http_challenge))
            .fallback(to_https)
            .with_state(self);
        Ok(crate::AbortOnDrop(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::warn!("ACME http-01 listener: {e}");
            }
        })))
    }
}

async fn http_challenge(
    State(acme): State<Arc<Acme>>,
    UrlPath(token): UrlPath<String>,
) -> Response {
    match acme.tokens.lock().unwrap().get(&token) {
        Some(answer) => (
            [(header::CONTENT_TYPE, "application/octet-stream")],
            answer.clone(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn to_https(State(acme): State<Arc<Acme>>, headers: HeaderMap, uri: Uri) -> Response {
    // Only to one of our own names: a Host header picks nothing else.
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|h| {
            h.rsplit_once(':')
                .map_or(h, |(n, _)| n)
                .to_ascii_lowercase()
        })
        .filter(|h| acme.config.domains.contains(h))
        .unwrap_or_else(|| acme.config.domains[0].clone());
    let path = uri.path_and_query().map_or("/", |p| p.as_str());
    Redirect::permanent(&format!("https://{host}{path}")).into_response()
}

fn break_invalid(auth: &Auth) -> Result<()> {
    let Identifier::Dns(name) = &auth.identifier;
    let detail = auth
        .challenges
        .iter()
        .find_map(|c| c.error.as_ref().and_then(|p| p.detail.clone()))
        .unwrap_or_else(|| format!("{:?}", auth.status));
    bail!("{name}: {detail}")
}

/// RFC 7638 thumbprint of the account key, for HTTP-01 answers.
fn jwk_thumbprint(account: &Account) -> String {
    let public = account.key_pair.public_key().as_ref();
    // Uncompressed point: 0x04, then x and y.
    let (x, y) = public[1..].split_at(32);
    let jwk = format!(
        r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
        BASE64URL_NOPAD.encode(x),
        BASE64URL_NOPAD.encode(y)
    );
    BASE64URL_NOPAD.encode(ring::digest::digest(&ring::digest::SHA256, jwk.as_bytes()).as_ref())
}

/// ACME servers turn a request away now and then with `badNonce`; the
/// client is meant to try again with a fresh one.
async fn retry_nonce<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tokio_rustls_acme::acme::AcmeError>>,
{
    let mut tries = 0;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if tries < 4 && e.to_string().contains("badNonce") => tries += 1,
            Err(e) => return Err(anyhow!("{e}")),
        }
    }
}

fn not_after(cert: &CertificateDer<'_>) -> Result<SystemTime> {
    use x509_cert::der::Decode;
    let cert = x509_cert::Certificate::from_der(cert).context("parse the certificate")?;
    Ok(UNIX_EPOCH + cert.tbs_certificate.validity.not_after.to_unix_duration())
}

fn fmt_time(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_default()
}

fn pem_encode(label: &str, der: &[u8]) -> String {
    let b64 = data_encoding::BASE64.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// The certificate directory, for `pumasi ui` (in the home).
pub fn default_cache(home: &Path) -> PathBuf {
    home.join("acme")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_names() {
        assert_eq!("http-01".parse::<Challenge>().unwrap(), Challenge::Http);
        assert_eq!(
            "tls-alpn-01".parse::<Challenge>().unwrap(),
            Challenge::TlsAlpn
        );
        assert!("dns-01".parse::<Challenge>().is_err());
    }

    #[test]
    fn refuses_addresses_and_bare_names() {
        let config = |d: &str| AcmeConfig {
            domains: vec![d.to_string()],
            email: None,
            directory: STAGING.to_string(),
            ca: None,
            cache: std::env::temp_dir(),
            challenge: Challenge::TlsAlpn,
            http_listen: "127.0.0.1:0".parse().unwrap(),
        };
        assert!(Acme::new(config("192.0.2.1")).is_err());
        assert!(Acme::new(config("localhost")).is_err());
        assert!(Acme::new(config("pumasi.example.com")).is_ok());
    }

    #[test]
    fn pem_round_trip() {
        let key = Account::generate_key_pair();
        let pem = pem_encode("PRIVATE KEY", &key);
        let back = PrivatePkcs8KeyDer::from_pem_slice(pem.as_bytes()).unwrap();
        assert_eq!(back.secret_pkcs8_der(), &key[..]);
    }

    /// Orders certificates from a local Pebble server with both
    /// challenges, when `PEBBLE` and `PEBBLE_CHALLTESTSRV` name its
    /// binaries (CI builds them; elsewhere the test passes without
    /// running).
    #[tokio::test]
    async fn orders_from_pebble() {
        let (Some(pebble), Some(dns)) = (
            std::env::var_os("PEBBLE"),
            std::env::var_os("PEBBLE_CHALLTESTSRV"),
        ) else {
            eprintln!("PEBBLE and PEBBLE_CHALLTESTSRV not set: skipped");
            return;
        };
        let dir = std::env::temp_dir().join(format!("pumasi-acme-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        // Pebble's own HTTPS certificate, which the client is told to trust.
        let key = rcgen::KeyPair::generate().unwrap();
        let cert =
            rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .unwrap()
                .self_signed(&key)
                .unwrap();
        std::fs::write(dir.join("api.pem"), cert.pem()).unwrap();
        std::fs::write(dir.join("api.key"), key.serialize_pem()).unwrap();
        let free = || {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let (api, mgmt, http, tls) = (free(), free(), free(), free());
        let dns_port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let config = serde_json::json!({"pebble": {
            "listenAddress": format!("127.0.0.1:{api}"),
            "managementListenAddress": format!("127.0.0.1:{mgmt}"),
            "certificate": dir.join("api.pem"),
            "privateKey": dir.join("api.key"),
            "httpPort": http,
            "tlsPort": tls,
            "ocspResponderURL": "",
            "externalAccountBindingRequired": false,
        }});
        std::fs::write(dir.join("pebble.json"), config.to_string()).unwrap();
        let mut dns = tokio::process::Command::new(dns)
            .args(["-defaultIPv4", "127.0.0.1", "-defaultIPv6", ""])
            .args(["-dns01", &format!("127.0.0.1:{dns_port}")])
            .args(["-management", &format!("127.0.0.1:{}", free())])
            .args(["-http01", "", "-https01", "", "-tlsalpn01", "", "-doh", ""])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut pebble = tokio::process::Command::new(pebble)
            .arg("-config")
            .arg(dir.join("pebble.json"))
            .args(["-dnsserver", &format!("127.0.0.1:{dns_port}")])
            .env("PEBBLE_VA_NOSLEEP", "1")
            .env("PEBBLE_WFE_NONCEREJECT", "0")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", api))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let root = pebble_root(&dir.join("api.pem"), mgmt).await;
        for (challenge, name) in [
            (Challenge::TlsAlpn, "alpn.pumasi.test"),
            (Challenge::Http, "http.pumasi.test"),
        ] {
            let acme = Acme::new(AcmeConfig {
                domains: vec![name.to_string()],
                email: Some("ops@pumasi.test".into()),
                directory: format!("https://localhost:{api}/dir"),
                ca: Some(dir.join("api.pem")),
                cache: dir.join("cache"),
                challenge,
                http_listen: format!("127.0.0.1:{http}").parse().unwrap(),
            })
            .unwrap();
            assert!(acme.not_after().is_none());
            // The dashboard's HTTPS port, where TLS-ALPN-01 is checked.
            // (The last round's listener closes as its task winds down.)
            let mut listener = None;
            for _ in 0..50 {
                match tokio::net::TcpListener::bind(("127.0.0.1", tls)).await {
                    Ok(l) => {
                        listener = Some(l);
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
            let listener = listener.expect("the HTTPS port");
            let listener = crate::tls::TlsListener::new(listener, acme.server_config()).unwrap();
            let app = axum::Router::new().route("/", get(|| async { "pumasi" }));
            let server = crate::AbortOnDrop(tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            }));
            let _http = match challenge {
                Challenge::Http => Some(acme.clone().serve_http().await.unwrap()),
                Challenge::TlsAlpn => None,
            };
            acme.renew()
                .await
                .unwrap_or_else(|e| panic!("{challenge:?}: {e:#}"));
            let until = acme.not_after().expect("a certificate");
            assert!(until > SystemTime::now() + Duration::from_secs(7 * 24 * 3600));
            // Browsers get it, and it chains to Pebble's root.
            let body = https_get(&root, name, tls).await;
            assert!(body.ends_with("pumasi"), "{body}");
            drop(server);
            // A restart serves the kept certificate without ordering.
            let again = Acme::new(AcmeConfig {
                domains: vec![name.to_string()],
                email: None,
                directory: format!("https://localhost:{api}/dir"),
                ca: Some(dir.join("api.pem")),
                cache: dir.join("cache"),
                challenge,
                http_listen: format!("127.0.0.1:{http}").parse().unwrap(),
            })
            .unwrap();
            assert_eq!(again.not_after(), Some(until));
        }
        let _ = pebble.kill().await;
        let _ = dns.kill().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    fn client(roots: rustls::RootCertStore) -> tokio_rustls::TlsConnector {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    async fn fetch(
        connector: tokio_rustls::TlsConnector,
        name: &str,
        port: u16,
        path: &str,
    ) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let server = rustls::pki_types::ServerName::try_from(name.to_string()).unwrap();
        let mut tls = connector.connect(server, tcp).await.unwrap();
        tls.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {name}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        let mut out = Vec::new();
        let _ = tls.read_to_end(&mut out).await;
        let text = String::from_utf8_lossy(&out).to_string();
        text.split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or(text)
    }

    /// The root Pebble signs with (it makes a new one at each start).
    async fn pebble_root(api_cert: &Path, mgmt: u16) -> rustls::RootCertStore {
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_file_iter(api_cert).unwrap() {
            roots.add(c.unwrap()).unwrap();
        }
        let pem = fetch(client(roots), "localhost", mgmt, "/roots/0").await;
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            roots.add(c.unwrap()).unwrap();
        }
        assert!(!roots.is_empty(), "no root from Pebble: {pem}");
        roots
    }

    async fn https_get(roots: &rustls::RootCertStore, name: &str, port: u16) -> String {
        fetch(client(roots.clone()), name, port, "/").await
    }
}
