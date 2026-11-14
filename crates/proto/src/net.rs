//! Endpoint construction and key storage shared by every component.

use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointAddr, RelayMap, RelayMode, SecretKey, endpoint::presets};

pub use iroh::RelayUrl;
use serde::{Deserialize, Serialize};

/// How an endpoint finds and reaches peers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetMode {
    /// Relays and global address lookup (works across NATs and the internet).
    #[default]
    Public,
    /// Direct IP only, no relays or lookup services. For a LAN or tests.
    Local,
}

impl std::str::FromStr for NetMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "public" => Ok(Self::Public),
            "local" => Ok(Self::Local),
            _ => anyhow::bail!("network mode must be `public` or `local`"),
        }
    }
}

/// Binds an iroh endpoint.
pub async fn bind_endpoint(
    secret_key: SecretKey,
    mode: NetMode,
    alpns: Vec<Vec<u8>>,
    bind_addr: Option<SocketAddr>,
) -> Result<Endpoint> {
    bind_endpoint_with(secret_key, mode, &[], alpns, bind_addr).await
}

/// [`bind_endpoint`] with its own relay servers in public mode (empty: n0's
/// public relays). Peers reach it through whichever relay its address
/// names, so only it needs to know them.
pub async fn bind_endpoint_with(
    secret_key: SecretKey,
    mode: NetMode,
    relays: &[RelayUrl],
    alpns: Vec<Vec<u8>>,
    bind_addr: Option<SocketAddr>,
) -> Result<Endpoint> {
    let mut builder = match mode {
        NetMode::Public => Endpoint::builder(presets::N0),
        NetMode::Local => Endpoint::builder(presets::Minimal),
    };
    if mode == NetMode::Public && !relays.is_empty() {
        builder = builder.relay_mode(RelayMode::Custom(RelayMap::from_iter(
            relays.iter().cloned(),
        )));
    }
    builder = builder.secret_key(secret_key).alpns(alpns);
    if mode == NetMode::Public {
        let via = ProxySettings::from_env()?;
        if let Some(url) = &via.proxy {
            tracing::debug!(proxy = %redact(url), "reaching relays through a proxy");
            builder = builder.proxy_url(url.clone());
        }
        if let Some(tls) = via.ca_tls_config() {
            builder = builder.ca_tls_config(tls);
        }
    }
    if let Some(addr) = bind_addr {
        builder = builder.bind_addr(addr).context("invalid bind address")?;
    }
    let endpoint = builder.bind().await.context("bind endpoint")?;
    Ok(endpoint)
}

/// The environment variable naming the proxy for relay connections, before
/// the usual `HTTPS_PROXY` and friends; `off` (or `none`, or empty) goes
/// direct even when those are set.
pub const PROXY_ENV: &str = "PUMASI_PROXY";
/// A PEM file of extra CA certificates trusted for relays and HTTPS
/// proxies (a proxy that opens TLS signs with its own CA).
pub const CA_FILE_ENV: &str = "PUMASI_CA_FILE";

/// How relay connections leave this machine (1.12): through an HTTP(S)
/// proxy with `CONNECT`, for clients such as cloud sandboxes that have no
/// other way out, and with extra trusted CAs for a proxy that re-signs
/// TLS. Only relay traffic (HTTPS) goes through the proxy; peers are then
/// reached through the relay, since a proxy carries no UDP.
#[derive(Debug, Clone, Default)]
pub struct ProxySettings {
    pub proxy: Option<url::Url>,
    /// Certificates from [`CA_FILE_ENV`].
    pub extra_roots: Vec<rustls::pki_types::CertificateDer<'static>>,
    /// Also trust the system store (and `SSL_CERT_FILE`): on with a
    /// proxy, whose CA is usually installed there.
    pub system_roots: bool,
}

impl ProxySettings {
    /// Reads [`PROXY_ENV`], then `HTTPS_PROXY`, `https_proxy`,
    /// `HTTP_PROXY`, `http_proxy` and `ALL_PROXY`, and [`CA_FILE_ENV`].
    pub fn from_env() -> Result<Self> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let pick = |names: &[&'static str]| -> Option<(&'static str, String)> {
            names
                .iter()
                .find_map(|n| get(n).map(|v| (*n, v.trim().to_string())))
        };
        let proxy = match pick(&[PROXY_ENV]) {
            Some((_, v)) if v.is_empty() || v == "off" || v == "none" => None,
            Some((n, v)) => Some(parse_proxy(n, &v)?),
            None => match pick(&[
                "HTTPS_PROXY",
                "https_proxy",
                "HTTP_PROXY",
                "http_proxy",
                "ALL_PROXY",
                "all_proxy",
            ]) {
                Some((_, v)) if v.is_empty() => None,
                // Set for other programs: one we cannot use is skipped
                // with a warning rather than stopping this one.
                Some((n, v)) => match parse_proxy(n, &v) {
                    Ok(u) => Some(u),
                    Err(e) => {
                        tracing::warn!("{e:#}; reaching relays directly");
                        None
                    }
                },
                None => None,
            },
        };
        let mut extra_roots = Vec::new();
        if let Some(path) = get(CA_FILE_ENV).filter(|p| !p.trim().is_empty()) {
            extra_roots = load_pem_certs(Path::new(path.trim()))
                .with_context(|| format!("{CA_FILE_ENV}={path}"))?;
        }
        Ok(Self {
            system_roots: proxy.is_some(),
            proxy,
            extra_roots,
        })
    }

    /// The CAs relays and proxies are checked against, when anything is
    /// added to the bundled web roots.
    pub fn ca_tls_config(&self) -> Option<iroh::tls::CaTlsConfig> {
        if self.extra_roots.is_empty() && !self.system_roots {
            return None;
        }
        let mut roots = self.extra_roots.clone();
        if self.system_roots {
            roots.extend(rustls_native_certs::load_native_certs().certs);
        }
        Some(iroh::tls::CaTlsConfig::embedded().with_extra_roots(roots))
    }
}

/// `http://[user:pass@]host:port` or `https://...` (iroh's relay client
/// speaks `CONNECT` to either); a bare `host:port` is taken as `http://`.
fn parse_proxy(var: &str, value: &str) -> Result<url::Url> {
    let with_scheme = if value.contains("://") {
        value.to_string()
    } else {
        format!("http://{value}")
    };
    let scheme = with_scheme
        .split_once("://")
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_default();
    anyhow::ensure!(
        scheme == "http" || scheme == "https",
        "{var}: only http:// and https:// proxies are supported (with CONNECT), not {scheme}://"
    );
    let url = url::Url::parse(&with_scheme)
        .map_err(|e| anyhow::anyhow!("{var}: not a proxy URL: {e}"))?;
    anyhow::ensure!(
        url.host_str().is_some(),
        "{var}: the proxy URL names no host"
    );
    Ok(url)
}

/// A proxy URL without its password, for logs.
pub fn redact(url: &url::Url) -> String {
    let mut u = url.clone();
    if !u.username().is_empty() || u.password().is_some() {
        let _ = u.set_username("***");
        let _ = u.set_password(None);
    }
    u.to_string()
}

/// Every certificate in a PEM file.
pub fn load_pem_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let certs = CertificateDer::pem_file_iter(path)
        .with_context(|| format!("read {}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("{} is not a PEM certificate file", path.display()))?;
    anyhow::ensure!(
        !certs.is_empty(),
        "{} holds no PEM certificates",
        path.display()
    );
    Ok(certs)
}

/// The relays in `addrs` that are not n0's public ones: those of a cluster
/// started with `--relay`, which its nodes and clients then use too.
pub fn custom_relays(addrs: &[EndpointAddr]) -> Vec<RelayUrl> {
    let n0 = iroh::defaults::prod::default_relay_map();
    let staging = iroh::defaults::staging::default_relay_map();
    let mut out: Vec<RelayUrl> = Vec::new();
    for url in addrs.iter().flat_map(|a| a.relay_urls()) {
        if !n0.contains(url) && !staging.contains(url) && !out.contains(url) {
            out.push(url.clone());
        }
    }
    out
}

/// Returns an address others can dial. In public mode waits (bounded) until a
/// relay is connected so the address works from behind NAT.
pub async fn dialable_addr(endpoint: &Endpoint, mode: NetMode) -> EndpointAddr {
    match mode {
        NetMode::Public => {
            let _ = tokio::time::timeout(Duration::from_secs(10), endpoint.online()).await;
        }
        NetMode::Local => {
            // Local addresses appear right after bind, but give the socket
            // layer a moment if they are not there yet.
            for _ in 0..50 {
                if endpoint.addr().ip_addrs().next().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    endpoint.addr()
}

/// Loads the node's secret key from `path`, creating it on first use.
pub fn load_or_create_key(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let bytes = data_encoding::HEXLOWER
            .decode(text.trim().as_bytes())
            .context("secret key file is not hex")?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("bad key length"))?;
        Ok(SecretKey::from_bytes(&bytes))
    } else {
        let key = SecretKey::generate();
        write_private(
            path,
            data_encoding::HEXLOWER.encode(&key.to_bytes()).as_bytes(),
        )?;
        Ok(key)
    }
}

/// Saves a key the way [`load_or_create_key`] reads it.
pub fn save_key(path: &Path, key: &SecretKey) -> Result<()> {
    write_private(
        path,
        data_encoding::HEXLOWER.encode(&key.to_bytes()).as_bytes(),
    )
}

/// Writes a file readable only by the current user (on Unix).
pub fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    std::fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod relay_tests {
    use super::*;

    #[test]
    fn custom_relays_skip_n0_and_duplicates() {
        let id = SecretKey::generate().public();
        let n0 = iroh::defaults::prod::default_na_east_relay().url.clone();
        let own: RelayUrl = "https://relay.example.com".parse().unwrap();
        let addrs = [
            EndpointAddr::new(id).with_relay_url(n0),
            EndpointAddr::new(id).with_relay_url(own.clone()),
            EndpointAddr::new(id).with_relay_url(own.clone()),
            EndpointAddr::new(id).with_ip_addr("127.0.0.1:7480".parse().unwrap()),
        ];
        assert_eq!(custom_relays(&addrs), vec![own]);
        assert!(custom_relays(&addrs[..1]).is_empty());
    }
}

#[cfg(test)]
mod proxy_tests {
    use std::collections::HashMap;

    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn proxy_from_the_usual_variables() {
        let p = ProxySettings::from_vars(vars(&[])).unwrap();
        assert!(p.proxy.is_none() && p.ca_tls_config().is_none());

        let p = ProxySettings::from_vars(vars(&[("https_proxy", "http://10.0.0.1:3128")])).unwrap();
        assert_eq!(p.proxy.unwrap().as_str(), "http://10.0.0.1:3128/");
        assert!(p.system_roots);

        // HTTPS_PROXY before HTTP_PROXY; a bare host:port is http://.
        let p = ProxySettings::from_vars(vars(&[
            ("HTTP_PROXY", "http://wrong:1"),
            ("HTTPS_PROXY", "proxy.corp:8080"),
        ]))
        .unwrap();
        assert_eq!(p.proxy.unwrap().as_str(), "http://proxy.corp:8080/");

        // PUMASI_PROXY wins, and `off` goes direct.
        let p = ProxySettings::from_vars(vars(&[
            ("HTTPS_PROXY", "http://a:1"),
            (PROXY_ENV, "https://user:pw@b:2"),
        ]))
        .unwrap();
        let url = p.proxy.unwrap();
        assert_eq!(url.username(), "user");
        assert_eq!(redact(&url), "https://***@b:2/");
        let p =
            ProxySettings::from_vars(vars(&[("HTTPS_PROXY", "http://a:1"), (PROXY_ENV, "off")]))
                .unwrap();
        assert!(p.proxy.is_none());
    }

    #[test]
    fn socks_is_refused_only_when_asked_for() {
        // Someone else's ALL_PROXY: skipped.
        let p =
            ProxySettings::from_vars(vars(&[("ALL_PROXY", "socks5://127.0.0.1:1080")])).unwrap();
        assert!(p.proxy.is_none());
        // Ours: an error.
        let e = ProxySettings::from_vars(vars(&[(PROXY_ENV, "socks5://127.0.0.1:1080")]))
            .unwrap_err()
            .to_string();
        assert!(e.contains("only http:// and https://"), "{e}");
    }

    #[test]
    fn extra_ca_file() {
        let dir = std::env::temp_dir().join(format!("pumasi-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        let e =
            ProxySettings::from_vars(vars(&[(CA_FILE_ENV, empty.to_str().unwrap())])).unwrap_err();
        assert!(format!("{e:#}").contains("no PEM certificates"), "{e:#}");
        // A real certificate (DER bytes need not parse as X.509 for PEM).
        let pem = dir.join("ca.pem");
        std::fs::write(
            &pem,
            "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUQ0E=\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let p = ProxySettings::from_vars(vars(&[(CA_FILE_ENV, pem.to_str().unwrap())])).unwrap();
        assert_eq!(p.extra_roots.len(), 1);
        assert!(!p.system_roots && p.ca_tls_config().is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
