//! A minimal OCI distribution client: resolves an image reference to the
//! manifest for a platform, and downloads its config and layer blobs,
//! checking every digest. Public images use anonymous bearer tokens (Docker
//! Hub, GHCR, Quay, ...); private registries take a user name and password
//! or token, sent as HTTP basic auth or exchanged for a bearer token.
//!
//! The agent uses it for public images in microVMs; the control plane uses
//! it with the cluster's registry secrets to pull private images into the
//! file store, so the credentials never leave the control plane.

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{StatusCode, header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

const MANIFEST_TYPES: &str = "application/vnd.oci.image.index.v1+json, \
application/vnd.docker.distribution.manifest.list.v2+json, \
application/vnd.oci.image.manifest.v1+json, \
application/vnd.docker.distribution.manifest.v2+json";
/// Manifests and configs are small; refuse anything absurd.
const MAX_JSON_BYTES: usize = 4 * 1024 * 1024;

/// `[registry/]repository[:tag][@digest]`, normalized like Docker does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    /// Host (and port) of the registry API.
    pub registry: String,
    pub repository: String,
    /// A tag or a `sha256:` digest.
    pub reference: String,
}

impl ImageRef {
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        ensure!(
            !s.is_empty() && !s.contains(char::is_whitespace),
            "invalid image reference {s:?}"
        );
        let (name, digest) = match s.split_once('@') {
            Some((n, d)) => (n, Some(d.to_string())),
            None => (s, None),
        };
        let (name, tag) = match name.rsplit_once(':') {
            Some((n, t)) if !t.contains('/') => (n, Some(t.to_string())),
            _ => (name, None),
        };
        let (registry, repository) = match name.split_once('/') {
            Some((first, rest))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (first.to_string(), rest.to_string())
            }
            _ => ("docker.io".to_string(), name.to_string()),
        };
        let registry = normalize_registry(&registry);
        let repository = if registry == "registry-1.docker.io" && !repository.contains('/') {
            format!("library/{repository}")
        } else {
            repository
        };
        ensure!(
            !repository.is_empty()
                && repository.split('/').all(|p| {
                    !p.is_empty()
                        && p.chars().all(|c| {
                            c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c)
                        })
                }),
            "invalid repository name in {s:?}"
        );
        let reference = match (digest, tag) {
            (Some(d), _) => {
                ensure!(
                    d.starts_with("sha256:") && d.len() == 71,
                    "unsupported digest in {s:?}"
                );
                d
            }
            (None, Some(t)) => t,
            (None, None) => "latest".into(),
        };
        Ok(Self {
            registry,
            repository,
            reference,
        })
    }

    /// `registry/repository:tag` or `@digest`, as Docker would print it.
    pub fn display(&self) -> String {
        let sep = if self.reference.starts_with("sha256:") {
            '@'
        } else {
            ':'
        };
        format!(
            "{}/{}{sep}{}",
            self.registry, self.repository, self.reference
        )
    }

    fn base_url(&self) -> String {
        let plain = self.registry.starts_with("localhost") || self.registry.starts_with("127.");
        let scheme = if plain { "http" } else { "https" };
        format!("{scheme}://{}/v2/{}", self.registry, self.repository)
    }
}

/// The registry host credentials are looked up by: Docker Hub's aliases
/// become its API host, everything else is kept (lowercased).
pub fn normalize_registry(host: &str) -> String {
    let host = host.trim().trim_end_matches('/').to_ascii_lowercase();
    let host = host
        .strip_prefix("https://")
        .or_else(|| host.strip_prefix("http://"))
        .unwrap_or(&host)
        .to_string();
    match host.as_str() {
        "docker.io" | "index.docker.io" | "registry-1.docker.io" => "registry-1.docker.io".into(),
        _ => host,
    }
}

/// A user name and a password (or access token) for one registry.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Credentials({}, <hidden>)", self.username)
    }
}

impl Credentials {
    /// Parses `user:password`. A value without a colon is a token used
    /// with the user name `token`.
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim_end_matches(['\r', '\n']);
        ensure!(!value.is_empty(), "empty registry credentials");
        Ok(match value.split_once(':') {
            Some((u, p)) => Self {
                username: u.to_string(),
                password: p.to_string(),
            },
            None => Self {
                username: "token".into(),
                password: value.to_string(),
            },
        })
    }
}

/// What a request carries for a repository.
#[derive(Clone)]
enum Auth {
    Bearer(String),
    Basic,
}

/// The parts of an image config the executor uses (`config` in the OCI
/// image config JSON).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct ContainerConfig {
    pub env: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    #[serde(default)]
    pub media_type: String,
    pub digest: String,
    pub size: u64,
    #[serde(default, skip_serializing)]
    platform: Option<Platform>,
}

#[derive(Debug, Clone, Deserialize)]
struct Platform {
    architecture: String,
    os: String,
    #[serde(default)]
    variant: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    #[serde(default)]
    media_type: String,
    #[serde(default)]
    manifests: Option<Vec<Descriptor>>,
    #[serde(default)]
    config: Option<Descriptor>,
    #[serde(default)]
    layers: Option<Vec<Descriptor>>,
}

#[derive(Deserialize)]
struct ConfigFile {
    #[serde(default)]
    architecture: String,
    #[serde(default)]
    config: Option<ContainerConfig>,
}

/// An image resolved for this platform.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub image: ImageRef,
    /// Digest of the platform manifest; identifies the image contents.
    pub digest: String,
    pub config: ContainerConfig,
    /// The raw image config and its digest (`docker load` needs both).
    pub config_digest: String,
    pub config_bytes: Vec<u8>,
    pub layers: Vec<Descriptor>,
}

#[derive(Clone)]
pub struct Registry {
    http: reqwest::Client,
    /// How requests authenticate, by (registry, repository).
    tokens: Arc<Mutex<HashMap<(String, String), Auth>>>,
    /// Credentials by normalized registry host.
    creds: Arc<HashMap<String, Credentials>>,
    /// The OCI architecture to resolve image indexes for.
    arch: String,
}

/// The OCI architecture name of this machine.
pub fn oci_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

/// The bundled web roots plus the system store (and `SSL_CERT_FILE`).
pub fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    roots
}

/// An HTTPS client trusting the bundled web roots plus the system store
/// (and `SSL_CERT_FILE`), for registries behind corporate proxies.
pub fn http_client() -> Result<reqwest::Client> {
    let roots = root_certs();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .user_agent(concat!("pumasi/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .build()
        .context("build HTTP client")
}

impl Registry {
    pub fn new() -> Result<Self> {
        Self::with_credentials(HashMap::new())
    }

    /// A client that logs in to these registries (keys are registry hosts,
    /// see [`normalize_registry`]).
    pub fn with_credentials(creds: HashMap<String, Credentials>) -> Result<Self> {
        Ok(Self {
            http: http_client()?,
            tokens: Default::default(),
            creds: Arc::new(
                creds
                    .into_iter()
                    .map(|(k, v)| (normalize_registry(&k), v))
                    .collect(),
            ),
            arch: oci_arch().to_string(),
        })
    }

    /// Resolves image indexes for another architecture (`amd64`, `arm64`).
    pub fn for_arch(mut self, arch: &str) -> Self {
        self.arch = arch.to_string();
        self
    }

    fn creds_for(&self, image: &ImageRef) -> Option<&Credentials> {
        self.creds.get(&image.registry)
    }

    /// GET `<base>/<path>`, fetching an anonymous bearer token when asked.
    async fn get(&self, image: &ImageRef, path: &str, accept: &str) -> Result<reqwest::Response> {
        let url = format!("{}/{path}", image.base_url());
        let key = (image.registry.clone(), image.repository.clone());
        let mut authed = false;
        let mut backoff = Duration::from_secs(2);
        let mut retries = 0;
        loop {
            let mut req = self.http.get(&url).header(header::ACCEPT, accept);
            let auth = self.tokens.lock().unwrap().get(&key).cloned();
            match auth {
                Some(Auth::Bearer(t)) => req = req.bearer_auth(t),
                Some(Auth::Basic) => {
                    if let Some(c) = self.creds_for(image) {
                        req = req.basic_auth(&c.username, Some(&c.password));
                    }
                }
                None => {}
            }
            let resp = req.send().await.with_context(|| format!("GET {url}"))?;
            let status = resp.status();
            // Rate limits and hiccups: wait and try again a few times.
            if (status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()) && retries < 4
            {
                tracing::warn!(%url, %status, "registry busy, retrying in {}s", backoff.as_secs());
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                retries += 1;
                continue;
            }
            if status == StatusCode::UNAUTHORIZED && !authed {
                authed = true;
                let challenge = resp
                    .headers()
                    .get(header::WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let auth = if challenge.to_ascii_lowercase().starts_with("basic") {
                    ensure!(
                        self.creds_for(image).is_some(),
                        "{} wants a login for {}; add it with `pumasi secret set registry/{}`",
                        image.registry,
                        image.repository,
                        image.registry
                    );
                    Auth::Basic
                } else {
                    Auth::Bearer(self.token(&challenge, image).await?)
                };
                self.tokens.lock().unwrap().insert(key.clone(), auth);
                continue;
            }
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                let hint = if self.creds_for(image).is_some() {
                    "check the credentials in the cluster secret".to_string()
                } else {
                    format!(
                        "for a private image, add a login with `pumasi secret set registry/{}`",
                        image.registry
                    )
                };
                bail!(
                    "{} refused access to {} ({status}); {hint}",
                    image.registry,
                    image.repository
                );
            }
            if status == StatusCode::NOT_FOUND {
                bail!(
                    "{}/{}:{} not found",
                    image.registry,
                    image.repository,
                    image.reference
                );
            }
            if !status.is_success() {
                let body = read_capped(resp, 64 * 1024).await.unwrap_or_default();
                let body = String::from_utf8_lossy(&body);
                bail!("GET {url}: {status}: {}", body.trim());
            }
            return Ok(resp);
        }
    }

    async fn token(&self, challenge: &str, image: &ImageRef) -> Result<String> {
        let Some(params) = challenge.strip_prefix("Bearer ") else {
            bail!(
                "{} answered with an unsupported login scheme for {}: {challenge}",
                image.registry,
                image.repository
            );
        };
        let mut realm = None;
        let mut query: Vec<(String, String)> = Vec::new();
        for (k, v) in parse_challenge(params) {
            match k.as_str() {
                "realm" => realm = Some(v),
                "service" | "scope" => query.push((k, v)),
                _ => {}
            }
        }
        if !query.iter().any(|(k, _)| k == "scope") {
            query.push((
                "scope".into(),
                format!("repository:{}:pull", image.repository),
            ));
        }
        let realm = realm.context("registry auth challenge has no realm")?;
        ensure!(
            realm.starts_with("https://") || realm.starts_with("http://localhost"),
            "registry token endpoint must use https: {realm}"
        );
        let mut url = reqwest::Url::parse(&realm).context("bad token endpoint")?;
        url.query_pairs_mut().extend_pairs(&query);
        let mut req = self.http.get(url);
        if let Some(c) = self.creds_for(image) {
            req = req.basic_auth(&c.username, Some(&c.password));
        }
        let resp = req.send().await.with_context(|| format!("GET {realm}"))?;
        if !resp.status().is_success() {
            let hint = if self.creds_for(image).is_some() {
                "check the credentials in the cluster secret".to_string()
            } else {
                format!(
                    "for a private image, add a login with `pumasi secret set registry/{}`",
                    image.registry
                )
            };
            bail!(
                "{} refused a token for {} ({}); {hint}",
                image.registry,
                image.repository,
                resp.status()
            );
        }
        #[derive(Deserialize)]
        struct Token {
            token: Option<String>,
            access_token: Option<String>,
        }
        let t: Token = serde_json::from_slice(&read_capped(resp, MAX_JSON_BYTES).await?)
            .context("bad token response")?;
        t.token
            .or(t.access_token)
            .context("token response has no token")
    }

    /// Resolves `reference` to this platform's manifest and reads its config.
    pub async fn resolve(&self, reference: &str) -> Result<Resolved> {
        let image = ImageRef::parse(reference)?;
        let (mut digest, mut manifest) = self.manifest(&image, &image.reference).await?;
        if let Some(list) = &manifest.manifests {
            let arch = self.arch.as_str();
            let pick = list
                .iter()
                .filter(|d| {
                    d.platform
                        .as_ref()
                        .is_some_and(|p| p.os == "linux" && p.architecture == arch)
                })
                .max_by_key(|d| {
                    d.platform
                        .as_ref()
                        .and_then(|p| p.variant.as_deref())
                        .is_some_and(|v| v == "v8")
                })
                .with_context(|| format!("{reference} has no linux/{arch} image"))?
                .digest
                .clone();
            (digest, manifest) = self.manifest(&image, &pick).await?;
            ensure!(
                manifest.manifests.is_none(),
                "{reference}: nested image index"
            );
        }
        ensure!(
            !manifest.media_type.contains("v1+prettyjws"),
            "{reference} uses the old schema 1 manifest format"
        );
        let config_desc = manifest
            .config
            .with_context(|| format!("{reference}: manifest has no config"))?;
        let layers = manifest.layers.unwrap_or_default();
        for l in &layers {
            ensure!(
                l.media_type.is_empty()
                    || l.media_type.contains("tar")
                    || l.media_type.contains("rootfs"),
                "{reference}: unsupported layer type {}",
                l.media_type
            );
        }
        let resp = self
            .get(&image, &format!("blobs/{}", config_desc.digest), "*/*")
            .await?;
        let body = read_capped(resp, MAX_JSON_BYTES).await?;
        check_digest(&config_desc.digest, &body)?;
        let config: ConfigFile = serde_json::from_slice(&body).context("bad image config")?;
        if !config.architecture.is_empty() && config.architecture != self.arch {
            bail!(
                "{reference} is built for {}, not {}",
                config.architecture,
                self.arch
            );
        }
        Ok(Resolved {
            image,
            digest,
            config: config.config.unwrap_or_default(),
            config_digest: config_desc.digest.clone(),
            config_bytes: body,
            layers,
        })
    }

    async fn manifest(&self, image: &ImageRef, reference: &str) -> Result<(String, Manifest)> {
        let resp = self
            .get(image, &format!("manifests/{reference}"), MANIFEST_TYPES)
            .await?;
        let body = read_capped(resp, MAX_JSON_BYTES).await?;
        let digest = format!("sha256:{}", hex(&Sha256::digest(&body)));
        if reference.starts_with("sha256:") {
            ensure!(
                digest == reference,
                "manifest digest mismatch for {reference}"
            );
        }
        let m: Manifest = serde_json::from_slice(&body).context("bad manifest")?;
        Ok((digest, m))
    }

    /// Downloads a blob to `dest`, checking its digest.
    pub async fn download(
        &self,
        image: &ImageRef,
        desc: &Descriptor,
        dest: &Path,
        cancel: &AtomicBool,
    ) -> Result<()> {
        let mut resp = self
            .get(image, &format!("blobs/{}", desc.digest), "*/*")
            .await?;
        let mut file = tokio::fs::File::create(dest)
            .await
            .with_context(|| format!("create {}", dest.display()))?;
        let mut hasher = Sha256::new();
        let mut n = 0u64;
        while let Some(chunk) = resp.chunk().await.context("download layer")? {
            if cancel.load(Ordering::Relaxed) {
                bail!("cancelled while pulling");
            }
            n += chunk.len() as u64;
            ensure!(
                n <= desc.size,
                "layer {} is larger than declared",
                desc.digest
            );
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        ensure!(n == desc.size, "layer {} was cut short", desc.digest);
        let got = format!("sha256:{}", hex(&hasher.finalize()));
        ensure!(
            got == desc.digest,
            "layer digest mismatch for {}",
            desc.digest
        );
        Ok(())
    }
}

async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        ensure!(out.len() + chunk.len() <= cap, "response too large");
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

fn check_digest(expected: &str, body: &[u8]) -> Result<()> {
    let got = format!("sha256:{}", hex(&Sha256::digest(body)));
    ensure!(got == expected, "digest mismatch: expected {expected}");
    Ok(())
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Splits `realm="...",service="..."` into pairs.
fn parse_challenge(s: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = s.trim();
    while !rest.is_empty() {
        let Some((key, after)) = rest.split_once('=') else {
            break;
        };
        let key = key
            .trim()
            .trim_start_matches(',')
            .trim()
            .to_ascii_lowercase();
        let (value, tail) = if let Some(q) = after.strip_prefix('"') {
            match q.find('"') {
                Some(end) => (q[..end].to_string(), &q[end + 1..]),
                None => (q.to_string(), ""),
            }
        } else {
            match after.find(',') {
                Some(end) => (after[..end].to_string(), &after[end..]),
                None => (after.to_string(), ""),
            }
        };
        out.push((key, value));
        rest = tail.trim_start_matches(',').trim();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_references_like_docker() {
        let r = ImageRef::parse("busybox").unwrap();
        assert_eq!(r.registry, "registry-1.docker.io");
        assert_eq!(r.repository, "library/busybox");
        assert_eq!(r.reference, "latest");
        let r = ImageRef::parse("python:3.12-slim").unwrap();
        assert_eq!(
            (r.repository.as_str(), r.reference.as_str()),
            ("library/python", "3.12-slim")
        );
        let r = ImageRef::parse("ghcr.io/acme/tool:1.0").unwrap();
        assert_eq!(
            (r.registry.as_str(), r.repository.as_str()),
            ("ghcr.io", "acme/tool")
        );
        let r = ImageRef::parse("localhost:5000/x/y").unwrap();
        assert_eq!(r.registry, "localhost:5000");
        assert_eq!(r.reference, "latest");
        assert!(r.base_url().starts_with("http://"));
        let d = format!("sha256:{}", "a".repeat(64));
        let r = ImageRef::parse(&format!("user/app@{d}")).unwrap();
        assert_eq!(r.repository, "user/app");
        assert_eq!(r.reference, d);
        for bad in ["", "a b", "UPPER/case", "x@md5:1", "a//b"] {
            assert!(ImageRef::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn parses_auth_challenges() {
        let c = parse_challenge(
            r#"realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/busybox:pull""#,
        );
        assert_eq!(
            c[0],
            ("realm".into(), "https://auth.docker.io/token".into())
        );
        assert_eq!(c[1].1, "registry.docker.io");
        assert_eq!(c[2].1, "repository:library/busybox:pull");
    }

    #[test]
    fn credentials_and_hosts() {
        let c = Credentials::parse("ci:s3cret:with:colons\n").unwrap();
        assert_eq!(
            (c.username.as_str(), c.password.as_str()),
            ("ci", "s3cret:with:colons")
        );
        let t = Credentials::parse("ghp_abc").unwrap();
        assert_eq!(t.username, "token");
        assert!(!format!("{c:?}").contains("s3cret"));
        assert_eq!(normalize_registry("docker.io"), "registry-1.docker.io");
        assert_eq!(normalize_registry("https://GHCR.io/"), "ghcr.io");
        assert_eq!(normalize_registry("localhost:5000"), "localhost:5000");
        let r = ImageRef::parse("localhost:5000/team/app:1").unwrap();
        assert_eq!(r.display(), "localhost:5000/team/app:1");
    }
}
