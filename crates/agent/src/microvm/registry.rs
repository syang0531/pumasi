//! A minimal OCI distribution client: resolves an image reference to the
//! manifest for this machine's platform, and downloads its config and
//! layer blobs, checking every digest. Anonymous pulls only (public images
//! on Docker Hub, GHCR, Quay and other registries that hand out anonymous
//! bearer tokens).

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
        let registry = match registry.as_str() {
            "docker.io" | "index.docker.io" => "registry-1.docker.io".to_string(),
            _ => registry,
        };
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

    fn base_url(&self) -> String {
        let plain = self.registry.starts_with("localhost") || self.registry.starts_with("127.");
        let scheme = if plain { "http" } else { "https" };
        format!("{scheme}://{}/v2/{}", self.registry, self.repository)
    }
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    #[serde(default)]
    pub media_type: String,
    pub digest: String,
    pub size: u64,
    #[serde(default)]
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
    pub layers: Vec<Descriptor>,
}

#[derive(Clone)]
pub struct Registry {
    http: reqwest::Client,
    /// Bearer tokens by (registry, repository).
    tokens: Arc<Mutex<HashMap<(String, String), String>>>,
}

/// The OCI architecture name of this machine.
pub fn oci_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

/// An HTTPS client trusting the bundled web roots plus the system store
/// (and `SSL_CERT_FILE`), for registries behind corporate proxies.
pub fn http_client() -> Result<reqwest::Client> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
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
        Ok(Self {
            http: http_client()?,
            tokens: Default::default(),
        })
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
            if let Some(t) = self.tokens.lock().unwrap().get(&key) {
                req = req.bearer_auth(t);
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
                let token = self.token(&challenge, image).await?;
                self.tokens.lock().unwrap().insert(key.clone(), token);
                continue;
            }
            if status == StatusCode::UNAUTHORIZED {
                bail!(
                    "{} refused access to {}; private images are not supported yet",
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
                "{} wants credentials for {}; only public images are supported",
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
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {realm}"))?;
        ensure!(
            resp.status().is_success(),
            "{} refused an anonymous token for {} ({}); private images are not supported yet",
            image.registry,
            image.repository,
            resp.status()
        );
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
            let arch = oci_arch();
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
        if !config.architecture.is_empty() && config.architecture != oci_arch() {
            bail!(
                "{reference} is built for {}, this machine is {}",
                config.architecture,
                oci_arch()
            );
        }
        Ok(Resolved {
            image,
            digest,
            config: config.config.unwrap_or_default(),
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
}
