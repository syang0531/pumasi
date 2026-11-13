//! Network probes between agents (1.6). The control plane asks one agent
//! to probe another ([`ToAgent::Probe`](pumasi_proto::msg::ToAgent)); it
//! connects on [`PROBE_ALPN`], times a few round trips of eight bytes, then
//! sends a short transfer and times its arrival. The other agent echoes
//! the round trips, counts the bytes, and answers with the count.
//!
//! The server side takes one probe at a time and a bounded number of bytes
//! a minute, so it cannot be used to flood the machine.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use pumasi_proto::{
    PROBE_ALPN,
    monitor::{PROBE_MAX_BYTES, PROBE_PINGS, ProbeResult},
};
use tokio::io::AsyncWriteExt;
use tracing::debug;

/// A whole probe, connecting included.
const PROBE_TIMEOUT: Duration = Duration::from_secs(25);
/// Bytes the server takes from all probes in one minute.
const BYTES_PER_MINUTE: u64 = 16 << 20;
/// Chunks the transfer is written and read in.
const CHUNK: usize = 64 << 10;

/// Answers other agents' probes.
#[derive(Debug, Clone)]
pub struct ProbeServer {
    busy: Arc<tokio::sync::Semaphore>,
    /// Start of the current minute and the bytes taken in it.
    budget: Arc<Mutex<(Instant, u64)>>,
}

impl Default for ProbeServer {
    fn default() -> Self {
        Self {
            busy: Arc::new(tokio::sync::Semaphore::new(1)),
            budget: Arc::new(Mutex::new((Instant::now(), 0))),
        }
    }
}

impl ProbeServer {
    /// Takes `bytes` from this minute's budget, or refuses.
    fn take(&self, bytes: u64) -> bool {
        let mut b = self.budget.lock().unwrap();
        if b.0.elapsed() >= Duration::from_secs(60) {
            *b = (Instant::now(), 0);
        }
        if b.1 + bytes > BYTES_PER_MINUTE {
            return false;
        }
        b.1 += bytes;
        true
    }
}

impl ProtocolHandler for ProbeServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let Ok(_one) = self.busy.clone().try_acquire_owned() else {
            conn.close(1u32.into(), b"busy");
            return Ok(());
        };
        match tokio::time::timeout(PROBE_TIMEOUT, serve(self, &conn)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => debug!(peer = %conn.remote_id().fmt_short(), "probe ended: {e:#}"),
            Err(_) => debug!(peer = %conn.remote_id().fmt_short(), "probe timed out"),
        }
        Ok(())
    }
}

async fn serve(server: &ProbeServer, conn: &Connection) -> Result<()> {
    let (mut send, mut recv) = conn.accept_bi().await?;
    let mut head = [0u8; 8];
    recv.read_exact(&mut head).await?;
    let pings = u32::from_le_bytes(head[..4].try_into().expect("4 bytes"));
    let bytes = u32::from_le_bytes(head[4..].try_into().expect("4 bytes"));
    ensure!(pings <= 4 * PROBE_PINGS, "too many round trips");
    ensure!(bytes <= PROBE_MAX_BYTES, "transfer too large");
    if !server.take(bytes as u64) {
        conn.close(2u32.into(), b"over budget");
        return Ok(());
    }
    for _ in 0..pings {
        let mut b = [0u8; 8];
        recv.read_exact(&mut b).await?;
        send.write_all(&b).await?;
        send.flush().await?;
    }
    let mut buf = vec![0u8; CHUNK];
    let mut left = bytes as usize;
    while left > 0 {
        let n = recv
            .read(&mut buf[..left.min(CHUNK)])
            .await?
            .context("the transfer stopped early")?;
        left -= n;
    }
    send.write_all(&(bytes as u64).to_le_bytes()).await?;
    send.finish()?;
    // The prober closes once it has the answer.
    let _ = tokio::time::timeout(Duration::from_secs(5), conn.closed()).await;
    Ok(())
}

/// Probes the agent at `addr` (node `node`): round trips, then `bytes`.
pub async fn probe(
    endpoint: &Endpoint,
    node: String,
    addr: EndpointAddr,
    bytes: u32,
) -> ProbeResult {
    let bytes = bytes.min(PROBE_MAX_BYTES);
    match tokio::time::timeout(PROBE_TIMEOUT, run(endpoint, addr, bytes)).await {
        Ok(Ok((rtt_us, micros, direct))) => ProbeResult {
            node,
            rtt_us: Some(rtt_us),
            bytes: bytes as u64,
            micros,
            direct: Some(direct),
            error: None,
        },
        Ok(Err(e)) => ProbeResult {
            node,
            error: Some(format!("{e:#}")),
            ..Default::default()
        },
        Err(_) => ProbeResult {
            node,
            error: Some("timed out".into()),
            ..Default::default()
        },
    }
}

/// (fastest round trip, transfer time, direct path), in microseconds.
async fn run(endpoint: &Endpoint, addr: EndpointAddr, bytes: u32) -> Result<(u32, u64, bool)> {
    let conn = endpoint
        .connect(addr, PROBE_ALPN)
        .await
        .context("connect")?;
    let result = async {
        let (mut send, mut recv) = conn.open_bi().await?;
        let mut head = [0u8; 8];
        head[..4].copy_from_slice(&PROBE_PINGS.to_le_bytes());
        head[4..].copy_from_slice(&bytes.to_le_bytes());
        send.write_all(&head).await?;
        let mut best = u64::MAX;
        for i in 0..PROBE_PINGS as u64 {
            let start = Instant::now();
            send.write_all(&i.to_le_bytes()).await?;
            send.flush().await?;
            let mut b = [0u8; 8];
            recv.read_exact(&mut b)
                .await
                .context("the other agent did not answer (busy, or older than 1.6)")?;
            ensure!(u64::from_le_bytes(b) == i, "wrong echo");
            best = best.min(start.elapsed().as_micros() as u64);
        }
        let chunk = vec![0u8; CHUNK];
        let start = Instant::now();
        let mut left = bytes as usize;
        while left > 0 {
            let n = left.min(CHUNK);
            send.write_all(&chunk[..n]).await?;
            left -= n;
        }
        send.flush().await?;
        let mut b = [0u8; 8];
        recv.read_exact(&mut b).await.context("no count back")?;
        let micros = start.elapsed().as_micros().max(1) as u64;
        ensure!(u64::from_le_bytes(b) == bytes as u64, "wrong count");
        let direct = conn
            .paths()
            .iter()
            .find(|p| p.is_selected())
            .is_some_and(|p| !p.is_relay());
        anyhow::Ok((best.min(u32::MAX as u64) as u32, micros, direct))
    }
    .await;
    conn.close(0u32.into(), b"done");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_server_keeps_to_its_budget() {
        let s = ProbeServer::default();
        assert!(s.take(PROBE_MAX_BYTES as u64));
        assert!(s.take(PROBE_MAX_BYTES as u64));
        assert!(s.take(PROBE_MAX_BYTES as u64));
        assert!(s.take(PROBE_MAX_BYTES as u64));
        assert!(!s.take(1), "16 MiB a minute");
    }

    #[tokio::test]
    async fn probes_another_endpoint() {
        use pumasi_proto::net::{NetMode, bind_endpoint_with};
        let key = || iroh::SecretKey::generate();
        let server =
            bind_endpoint_with(key(), NetMode::Local, &[], vec![PROBE_ALPN.to_vec()], None)
                .await
                .unwrap();
        let router = iroh::protocol::Router::builder(server.clone())
            .accept(PROBE_ALPN, ProbeServer::default())
            .spawn();
        let client = bind_endpoint_with(key(), NetMode::Local, &[], vec![], None)
            .await
            .unwrap();
        let r = probe(&client, "other".into(), server.addr(), 256 << 10).await;
        assert_eq!(r.error, None);
        assert!(r.rtt_us.is_some());
        assert_eq!(r.bytes, 256 << 10);
        assert!(r.bps().unwrap() > 0);
        assert_eq!(r.direct, Some(true));
        router.shutdown().await.unwrap();
    }
}
