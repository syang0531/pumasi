//! HTTPS for `pumasi ui --tls-cert --tls-key` (phase 15): the dashboard
//! serves TLS itself when no tunnel or reverse proxy does. Certificates
//! come from PEM files (from Let's Encrypt via certbot, a company CA, or
//! `mkcert`); getting them is left to those tools.
//!
//! Handshakes run in their own tasks, so a client that opens a
//! connection and says nothing holds up nobody else.

use std::{io, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

/// How long a client gets to finish the handshake.
const HANDSHAKE: Duration = Duration::from_secs(10);
/// Handshakes in flight at most; more connections wait in the kernel.
const MAX_HANDSHAKES: usize = 256;

/// Reads a certificate chain and its private key.
pub fn server_config(cert: &Path, key: &Path) -> Result<Arc<rustls::ServerConfig>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .with_context(|| format!("read {}", cert.display()))?
        .collect::<Result<_, _>>()
        .with_context(|| format!("parse the certificates in {}", cert.display()))?;
    anyhow::ensure!(!certs.is_empty(), "no certificate in {}", cert.display());
    let key = PrivateKeyDer::from_pem_file(key)
        .with_context(|| format!("read the private key in {}", key.display()))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("the certificate and key do not match")?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// A listener that hands out finished TLS connections.
pub struct TlsListener {
    local: SocketAddr,
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    _accept: crate::AbortOnDrop,
}

impl TlsListener {
    pub fn new(listener: TcpListener, config: Arc<rustls::ServerConfig>) -> io::Result<Self> {
        let local = listener.local_addr()?;
        let (tx, rx) = mpsc::channel(64);
        let acceptor = TlsAcceptor::from(config);
        let slots = Arc::new(tokio::sync::Semaphore::new(MAX_HANDSHAKES));
        let task = tokio::spawn(async move {
            loop {
                let (tcp, peer) = match listener.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        // Out of file descriptors and the like: wait a bit.
                        tracing::debug!("accept: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let Ok(slot) = slots.clone().acquire_owned().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    let _ = tcp.set_nodelay(true);
                    match tokio::time::timeout(HANDSHAKE, acceptor.accept(tcp)).await {
                        Ok(Ok(tls)) => {
                            let _ = tx.send((tls, peer)).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, "TLS handshake: {e}"),
                        Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(Self {
            local,
            rx,
            _accept: crate::AbortOnDrop(task),
        })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(c) => c,
            // The accept task only ends when the listener is dropped.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_or_empty_files() {
        let dir = std::env::temp_dir().join(format!("pumasi-tls-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        assert!(server_config(&cert, &key).is_err());
        std::fs::write(&cert, "").unwrap();
        std::fs::write(&key, "").unwrap();
        let e = server_config(&cert, &key).unwrap_err();
        assert!(format!("{e:#}").contains("no certificate"), "{e:#}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
