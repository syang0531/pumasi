//! The certificates of the WSL Docker daemon: a CA made for this machine,
//! a server certificate for `127.0.0.1`/`localhost` that `dockerd`
//! serves, and a client certificate the agent's Docker CLI presents
//! (`dockerd --tlsverify` accepts no other). ECDSA P-256, made with rcgen
//! when `pumasi docker setup` first runs.

use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};

/// What `dockerd` needs, as PEM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPems {
    pub ca: String,
    pub cert: String,
    pub key: String,
}

/// A CA with one server and one client certificate.
pub struct Issued {
    pub ca: rcgen::Certificate,
    pub server: (rcgen::Certificate, KeyPair),
    pub client: (rcgen::Certificate, KeyPair),
}

impl Issued {
    pub fn server_pems(&self) -> ServerPems {
        ServerPems {
            ca: self.ca.pem(),
            cert: self.server.0.pem(),
            key: self.server.1.serialize_pem(),
        }
    }
}

/// A new CA, server and client certificate.
pub fn issue() -> Result<Issued> {
    let ca_key = KeyPair::generate()?;
    let mut ca = CertificateParams::new(Vec::<String>::new())?;
    ca.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca.distinguished_name
        .push(DnType::CommonName, "Pumasi WSL Docker CA");
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_cert = ca.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca, ca_key);

    let server_key = KeyPair::generate()?;
    let mut server = CertificateParams::new(vec!["localhost".to_string()])?;
    server
        .subject_alt_names
        .push(SanType::IpAddress(std::net::Ipv4Addr::LOCALHOST.into()));
    server
        .distinguished_name
        .push(DnType::CommonName, "Pumasi WSL Docker daemon");
    server.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server.signed_by(&server_key, &issuer)?;

    let client_key = KeyPair::generate()?;
    let mut client = CertificateParams::new(Vec::<String>::new())?;
    client
        .distinguished_name
        .push(DnType::CommonName, "Pumasi agent");
    client.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client.signed_by(&client_key, &issuer)?;

    Ok(Issued {
        ca: ca_cert,
        server: (server_cert, server_key),
        client: (client_cert, client_key),
    })
}

/// Writes the server files (`ca.pem`, `server-cert.pem`, `server-key.pem`)
/// and the client files the Docker CLI reads (`ca.pem`, `cert.pem`,
/// `key.pem`).
pub fn write(issued: &Issued, server_dir: &Path, client_dir: &Path) -> Result<()> {
    let s = issued.server_pems();
    for (dir, files) in [
        (
            server_dir,
            [
                ("ca.pem", s.ca.clone()),
                ("server-cert.pem", s.cert),
                ("server-key.pem", s.key),
            ],
        ),
        (
            client_dir,
            [
                ("ca.pem", s.ca),
                ("cert.pem", issued.client.0.pem()),
                ("key.pem", issued.client.1.serialize_pem()),
            ],
        ),
    ] {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        for (name, pem) in files {
            let path = dir.join(name);
            std::fs::write(&path, pem).with_context(|| format!("write {}", path.display()))?;
        }
    }
    Ok(())
}

/// Reads the server files [`write`] wrote.
pub fn read_server(server_dir: &Path) -> Result<ServerPems> {
    let read = |name: &str| {
        let path = server_dir.join(name);
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))
    };
    Ok(ServerPems {
        ca: read("ca.pem")?,
        cert: read("server-cert.pem")?,
        key: read("server-key.pem")?,
    })
}

/// Whether both directories hold a complete set.
pub fn present(server_dir: &Path, client_dir: &Path) -> bool {
    ["ca.pem", "server-cert.pem", "server-key.pem"]
        .iter()
        .all(|f| server_dir.join(f).is_file())
        && ["ca.pem", "cert.pem", "key.pem"]
            .iter()
            .all(|f| client_dir.join(f).is_file())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::{
        ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
        server::WebPkiClientVerifier,
    };

    use super::*;

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    /// One TLS 1.3 handshake in memory; `Err` names the side that refused.
    fn handshake(
        ca: &rcgen::Certificate,
        server: (&rcgen::Certificate, &KeyPair),
        client: Option<(&rcgen::Certificate, &KeyPair)>,
        name: &str,
    ) -> Result<(), String> {
        let der = |c: &rcgen::Certificate| CertificateDer::from(c.der().to_vec());
        let key = |k: &KeyPair| PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(k.serialize_der()));
        let mut roots = RootCertStore::empty();
        roots.add(der(ca)).unwrap();
        let roots = Arc::new(roots);
        let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider())
            .build()
            .unwrap();
        let server_config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![der(server.0)], key(server.1))
            .unwrap();
        let builder = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
        let client_config = match client {
            Some((c, k)) => builder.with_client_auth_cert(vec![der(c)], key(k)).unwrap(),
            None => builder.with_no_client_auth(),
        };
        let mut c = ClientConnection::new(
            Arc::new(client_config),
            ServerName::try_from(name.to_string()).unwrap(),
        )
        .unwrap();
        let mut s = ServerConnection::new(Arc::new(server_config)).unwrap();
        for _ in 0..10 {
            let mut buf = Vec::new();
            while c.wants_write() {
                c.write_tls(&mut buf).unwrap();
            }
            let mut rd = &buf[..];
            while !rd.is_empty() {
                s.read_tls(&mut rd).unwrap();
            }
            s.process_new_packets()
                .map_err(|e| format!("server: {e}"))?;
            let mut buf = Vec::new();
            while s.wants_write() {
                s.write_tls(&mut buf).unwrap();
            }
            let mut rd = &buf[..];
            while !rd.is_empty() {
                c.read_tls(&mut rd).unwrap();
            }
            c.process_new_packets()
                .map_err(|e| format!("client: {e}"))?;
            if !c.is_handshaking() && !s.is_handshaking() {
                return Ok(());
            }
        }
        Err("handshake did not finish".into())
    }

    #[test]
    fn mutual_tls_with_the_issued_certificates() {
        let i = issue().unwrap();
        let server = (&i.server.0, &i.server.1);
        let client = (&i.client.0, &i.client.1);
        handshake(&i.ca, server, Some(client), "127.0.0.1").unwrap();
        handshake(&i.ca, server, Some(client), "localhost").unwrap();
        // The certificate is for the loopback only.
        assert!(handshake(&i.ca, server, Some(client), "192.168.1.5").is_err());
        // No client certificate, or one from another CA: refused.
        let err = handshake(&i.ca, server, None, "127.0.0.1").unwrap_err();
        assert!(err.starts_with("server"), "{err}");
        let other = issue().unwrap();
        let err = handshake(
            &i.ca,
            server,
            Some((&other.client.0, &other.client.1)),
            "127.0.0.1",
        )
        .unwrap_err();
        assert!(err.starts_with("server"), "{err}");
        // The server certificate cannot stand in for a client one.
        assert!(handshake(&i.ca, server, Some(server), "127.0.0.1").is_err());
    }

    #[test]
    fn files() {
        let dir = crate::tempdir::TempDir::new("wsl-tls").unwrap();
        let (s, c) = (dir.path().join("server"), dir.path().join("client"));
        assert!(!present(&s, &c));
        let i = issue().unwrap();
        write(&i, &s, &c).unwrap();
        assert!(present(&s, &c));
        let pems = read_server(&s).unwrap();
        assert_eq!(pems, i.server_pems());
        assert!(pems.key.contains("PRIVATE KEY"));
        assert_eq!(
            std::fs::read_to_string(c.join("ca.pem")).unwrap(),
            pems.ca,
            "both sides trust the same CA"
        );
    }
}
