//! Endpoint construction and key storage shared by every component.

use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointAddr, SecretKey, endpoint::presets};
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
    let mut builder = match mode {
        NetMode::Public => Endpoint::builder(presets::N0),
        NetMode::Local => Endpoint::builder(presets::Minimal),
    };
    builder = builder.secret_key(secret_key).alpns(alpns);
    if let Some(addr) = bind_addr {
        builder = builder.bind_addr(addr).context("invalid bind address")?;
    }
    let endpoint = builder.bind().await.context("bind endpoint")?;
    Ok(endpoint)
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
