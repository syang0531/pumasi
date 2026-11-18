//! Email alerts (1.22): a small SMTP client for one message at a time.
//!
//! `smtp://` connects in the clear and upgrades with STARTTLS, which the
//! server must offer unless it is this machine (a local relay such as
//! Postfix on the control plane's own host); `smtps://` speaks TLS from the
//! first byte. Credentials go only over TLS (or to a loopback server), with
//! `AUTH PLAIN` where offered, else `AUTH LOGIN`. The message is plain text
//! in UTF-8, its body in base64 and its subject as an RFC 2047 word, so no
//! server needs 8BITMIME.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use data_encoding::BASE64;
use pumasi_proto::monitor::{MailTarget, MailTls};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// How long the whole exchange may take.
const TIMEOUT: Duration = Duration::from_secs(30);

/// TLS settings that trust the usual roots.
pub fn client_tls() -> Result<Arc<rustls::ClientConfig>> {
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(pumasi_proto::registry::root_certs())
        .with_no_client_auth(),
    ))
}

/// Sends one message.
pub async fn send(
    target: &MailTarget,
    subject: &str,
    body: &str,
    tls: Arc<rustls::ClientConfig>,
) -> Result<()> {
    tokio::time::timeout(TIMEOUT, exchange(target, subject, body, tls))
        .await
        .context("the mail server did not answer in time")?
}

async fn exchange(
    target: &MailTarget,
    subject: &str,
    body: &str,
    tls: Arc<rustls::ClientConfig>,
) -> Result<()> {
    let tcp = TcpStream::connect((target.host.as_str(), target.port))
        .await
        .with_context(|| format!("connect to {}:{}", target.host, target.port))?;
    let message = message(target, subject, body);
    match target.tls {
        MailTls::Implicit => {
            let s = tls_connect(tcp, &target.host, tls).await?;
            let mut c = Smtp::new(s);
            c.expect(220).await?;
            let caps = c.ehlo().await?;
            c.deliver(target, &caps, true, &message).await
        }
        MailTls::StartTls => {
            let mut c = Smtp::new(tcp);
            c.expect(220).await?;
            let caps = c.ehlo().await?;
            if caps.iter().any(|l| l.eq_ignore_ascii_case("STARTTLS")) {
                c.cmd("STARTTLS", 220).await?;
                let tcp = c.into_inner()?;
                let s = tls_connect(tcp, &target.host, tls).await?;
                let mut c = Smtp::new(s);
                let caps = c.ehlo().await?;
                c.deliver(target, &caps, true, &message).await
            } else if target.loopback() {
                c.deliver(target, &caps, false, &message).await
            } else {
                bail!(
                    "{} offers no STARTTLS; use smtps:// if it speaks TLS on another port",
                    target.host
                )
            }
        }
    }
}

async fn tls_connect(
    tcp: TcpStream,
    host: &str,
    tls: Arc<rustls::ClientConfig>,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .with_context(|| format!("{host:?} is not a server name"))?;
    tokio_rustls::TlsConnector::from(tls)
        .connect(name, tcp)
        .await
        .with_context(|| format!("TLS with {host}"))
}

/// The message: headers and the body in base64.
fn message(target: &MailTarget, subject: &str, body: &str) -> String {
    let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S +0000");
    let id = data_encoding::HEXLOWER.encode(&rand::random::<[u8; 12]>());
    let domain = target.from.rsplit('@').next().unwrap_or("pumasi");
    let mut m = String::new();
    m.push_str(&format!("From: {}\r\n", target.from));
    m.push_str(&format!("To: {}\r\n", target.to.join(", ")));
    m.push_str(&format!("Subject: {}\r\n", encode_word(subject)));
    m.push_str(&format!("Date: {date}\r\n"));
    m.push_str(&format!("Message-ID: <{id}@{domain}>\r\n"));
    m.push_str("MIME-Version: 1.0\r\n");
    m.push_str("Content-Type: text/plain; charset=utf-8\r\n");
    m.push_str("Content-Transfer-Encoding: base64\r\n");
    m.push_str("Auto-Submitted: auto-generated\r\n\r\n");
    let b64 = BASE64.encode(body.as_bytes());
    for chunk in b64.as_bytes().chunks(76) {
        m.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
        m.push_str("\r\n");
    }
    m
}

/// A header value: as it is in ASCII, else an RFC 2047 encoded word.
fn encode_word(s: &str) -> String {
    let s: String = s.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    if s.is_ascii() {
        return s;
    }
    format!("=?UTF-8?B?{}?=", BASE64.encode(s.as_bytes()))
}

struct Smtp<S> {
    io: BufReader<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Smtp<S> {
    fn new(s: S) -> Self {
        Self {
            io: BufReader::new(s),
        }
    }

    fn into_inner(self) -> Result<S> {
        ensure!(
            self.io.buffer().is_empty(),
            "the mail server sent more before TLS"
        );
        Ok(self.io.into_inner())
    }

    /// One reply: its code and the text of each line.
    async fn reply(&mut self) -> Result<(u16, Vec<String>)> {
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            let n = self.io.read_line(&mut line).await?;
            ensure!(n > 0, "the mail server closed the connection");
            ensure!(line.len() <= 4096, "a mail server reply line is too long");
            let line = line.trim_end_matches(['\r', '\n']);
            ensure!(
                line.len() >= 3,
                "a mail server reply without a code: {line:?}"
            );
            let code: u16 = line[..3]
                .parse()
                .with_context(|| format!("a mail server reply without a code: {line:?}"))?;
            let more = line.as_bytes().get(3) == Some(&b'-');
            lines.push(line.get(4..).unwrap_or("").to_string());
            if !more {
                return Ok((code, lines));
            }
            ensure!(lines.len() < 100, "a mail server reply is too long");
        }
    }

    async fn expect(&mut self, want: u16) -> Result<Vec<String>> {
        let (code, lines) = self.reply().await?;
        ensure!(
            code == want,
            "the mail server answered {code} {}",
            lines.join(" ")
        );
        Ok(lines)
    }

    async fn send_line(&mut self, line: &str) -> Result<()> {
        let w = self.io.get_mut();
        w.write_all(line.as_bytes()).await?;
        w.write_all(b"\r\n").await?;
        w.flush().await?;
        Ok(())
    }

    async fn cmd(&mut self, line: &str, want: u16) -> Result<Vec<String>> {
        self.send_line(line).await?;
        self.expect(want).await
    }

    /// EHLO, and the extensions the server lists.
    async fn ehlo(&mut self) -> Result<Vec<String>> {
        let lines = self.cmd("EHLO pumasi", 250).await?;
        Ok(lines.into_iter().skip(1).collect())
    }

    async fn deliver(
        &mut self,
        target: &MailTarget,
        caps: &[String],
        secure: bool,
        message: &str,
    ) -> Result<()> {
        if let Some(user) = &target.user {
            ensure!(
                secure || target.loopback(),
                "credentials go to the mail server only over TLS"
            );
            let pass = target.password.clone().unwrap_or_default();
            let auth = caps
                .iter()
                .find(|l| l.to_ascii_uppercase().starts_with("AUTH"))
                .map(|l| l.to_ascii_uppercase())
                .unwrap_or_default();
            if auth.split_whitespace().any(|m| m == "PLAIN") || !auth.contains("LOGIN") {
                let token = BASE64.encode(format!("\0{user}\0{pass}").as_bytes());
                self.cmd(&format!("AUTH PLAIN {token}"), 235)
                    .await
                    .context("the mail server refused the user name or password")?;
            } else {
                self.cmd("AUTH LOGIN", 334).await?;
                self.cmd(&BASE64.encode(user.as_bytes()), 334).await?;
                self.cmd(&BASE64.encode(pass.as_bytes()), 235)
                    .await
                    .context("the mail server refused the user name or password")?;
            }
        }
        self.cmd(&format!("MAIL FROM:<{}>", target.from), 250)
            .await?;
        for to in &target.to {
            self.cmd(&format!("RCPT TO:<{to}>"), 250)
                .await
                .with_context(|| format!("recipient {to}"))?;
        }
        self.cmd("DATA", 354).await?;
        let mut data = String::with_capacity(message.len() + 8);
        for line in message.split("\r\n") {
            // Dot-stuffing; base64 and the headers never start with one,
            // but a subject could.
            if line.starts_with('.') {
                data.push('.');
            }
            data.push_str(line);
            data.push_str("\r\n");
        }
        let data = data.trim_end_matches("\r\n").to_string();
        self.send_line(&data).await?;
        self.cmd(".", 250).await?;
        let _ = self.send_line("QUIT").await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_and_bodies_are_encoded() {
        assert_eq!(encode_word("plain"), "plain");
        assert_eq!(encode_word("a\r\nBcc: x"), "aBcc: x");
        assert_eq!(encode_word("노드"), "=?UTF-8?B?64W465Oc?=");
        let t = MailTarget::parse("smtp://127.0.0.1:25?from=p@x.org&to=a@x.org,b@x.org").unwrap();
        let m = message(&t, "[home] 노드 imac offline", "본문\nline");
        assert!(m.contains("To: a@x.org, b@x.org\r\n"));
        assert!(m.contains("Content-Transfer-Encoding: base64\r\n"));
        assert!(m.contains("Auto-Submitted: auto-generated\r\n\r\n"));
        let body = m.split("\r\n\r\n").nth(1).unwrap().replace("\r\n", "");
        assert_eq!(
            BASE64.decode(body.as_bytes()).unwrap(),
            "본문\nline".as_bytes()
        );
    }

    /// A STARTTLS server: it offers the upgrade in the clear and AUTH only
    /// after it, and gives back what came over TLS.
    #[tokio::test]
    async fn starttls_upgrades_before_credentials() {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let der = cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::try_from(signing_key.serialize_der()).unwrap();
        let server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![der.clone()], key)
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut io = BufReader::new(tcp);
            let mut line = String::new();
            io.get_mut().write_all(b"220 hi\r\n").await.unwrap();
            io.read_line(&mut line).await.unwrap();
            assert!(line.starts_with("EHLO"));
            io.get_mut()
                .write_all(b"250-hi\r\n250 STARTTLS\r\n")
                .await
                .unwrap();
            line.clear();
            io.read_line(&mut line).await.unwrap();
            assert_eq!(line, "STARTTLS\r\n");
            io.get_mut().write_all(b"220 go\r\n").await.unwrap();
            let tls = acceptor.accept(io.into_inner()).await.unwrap();
            let mut io = BufReader::new(tls);
            let mut seen = Vec::new();
            io.get_mut().flush().await.unwrap();
            loop {
                line.clear();
                if io.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let l = line.trim_end().to_string();
                let reply: &[u8] = if l.starts_with("EHLO") {
                    b"250-hi\r\n250 AUTH PLAIN LOGIN\r\n"
                } else if l.starts_with("AUTH") {
                    b"235 ok\r\n"
                } else if l == "DATA" {
                    b"354 go\r\n"
                } else if l == "QUIT" {
                    seen.push(l);
                    break;
                } else if seen.last().is_some_and(|p| p == "DATA") && l != "." {
                    // Message lines get no answer until the dot.
                    continue;
                } else {
                    b"250 ok\r\n"
                };
                seen.push(l);
                io.get_mut().write_all(reply).await.unwrap();
                io.get_mut().flush().await.unwrap();
            }
            seen
        });
        let target = MailTarget::parse(&format!(
            "smtp://u:p@localhost:{port}?from=p@x.org&to=a@x.org"
        ))
        .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).unwrap();
        let tls = Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
        );
        send(&target, "s", "b", tls).await.unwrap();
        let seen = srv.await.unwrap();
        assert_eq!(seen[0], "EHLO pumasi");
        assert_eq!(seen[1], format!("AUTH PLAIN {}", BASE64.encode(b"\0u\0p")));
        assert_eq!(seen[2], "MAIL FROM:<p@x.org>");
        assert_eq!(seen[3], "RCPT TO:<a@x.org>");
        assert_eq!(seen[4], "DATA");
        assert_eq!(seen.last().unwrap(), "QUIT");
    }
}
