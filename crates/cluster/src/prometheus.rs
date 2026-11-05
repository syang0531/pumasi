//! The control plane's HTTP side (phase 11): the opt-in Prometheus
//! `/metrics` endpoint, and the worker that posts alerts to webhooks.

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tracing::debug;

use crate::{monitor::Delivery, server::Inner};

/// Largest request head read.
const MAX_HEAD: usize = 16 * 1024;
/// Tries per webhook delivery and the waits between them.
const RETRY_WAITS: [Duration; 2] = [Duration::from_secs(2), Duration::from_secs(10)];

/// Serves `/metrics` (and `/healthz`) until the task is aborted.
pub(crate) async fn serve(inner: Arc<Inner>, listener: TcpListener, token: Option<String>) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let inner = inner.clone();
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(inner, stream, token.as_deref()).await {
                debug!(%peer, "metrics request: {e:#}");
            }
        });
    }
}

async fn handle(inner: Arc<Inner>, mut stream: TcpStream, token: Option<&str>) -> Result<()> {
    let mut head = Vec::new();
    let mut buf = [0u8; 2048];
    let read = async {
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            head.extend_from_slice(&buf[..n]);
            anyhow::ensure!(head.len() <= MAX_HEAD, "request head too large");
        }
        anyhow::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .context("request timed out")??;
    let text = String::from_utf8_lossy(&head);
    let mut lines = text.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let path = target.split('?').next().unwrap_or("");
    let authorized = match token {
        None => true,
        Some(t) => lines.any(|l| {
            l.split_once(':').is_some_and(|(k, v)| {
                k.trim().eq_ignore_ascii_case("authorization")
                    && v.trim()
                        .strip_prefix("Bearer ")
                        .is_some_and(|got| pumasi_proto::secrets_eq_str(got.trim(), t))
            })
        }),
    };
    let (status, kind, body) = match (method, path) {
        ("GET" | "HEAD", _) if !authorized => (
            "401 Unauthorized",
            "text/plain; charset=utf-8",
            "a bearer token is required\n".to_string(),
        ),
        ("GET" | "HEAD", "/metrics") => {
            let text = inner.state().prometheus();
            ("200 OK", "text/plain; version=0.0.4; charset=utf-8", text)
        }
        ("GET" | "HEAD", "/healthz") => ("200 OK", "text/plain; charset=utf-8", "ok\n".into()),
        ("GET" | "HEAD", "/") => (
            "200 OK",
            "text/plain; charset=utf-8",
            "pumasi control plane: metrics are at /metrics\n".into(),
        ),
        ("GET" | "HEAD", _) => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".into(),
        ),
        _ => (
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            "only GET\n".into(),
        ),
    };
    let mut resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if status.starts_with("401") {
        resp.push_str("WWW-Authenticate: Bearer\r\n");
    }
    resp.push_str("\r\n");
    stream.write_all(resp.as_bytes()).await?;
    if method != "HEAD" {
        stream.write_all(body.as_bytes()).await?;
    }
    stream.shutdown().await.ok();
    Ok(())
}

/// Posts alerts to webhooks as the state machine queues them.
pub(crate) async fn webhook_worker(inner: Arc<Inner>, mut rx: mpsc::UnboundedReceiver<Delivery>) {
    while let Some(d) = rx.recv().await {
        deliver(inner.clone(), d);
    }
}

/// Posts one delivery (retrying twice) in the background, then records
/// how it went.
pub(crate) fn deliver(inner: Arc<Inner>, d: Delivery) {
    tokio::spawn(async move {
        let result = post(&d.url, &d.body).await;
        inner.state().on_delivery(&d.webhook, result.clone());
        if let Some(reply) = d.reply {
            let _ = reply.send(result);
        }
    });
}

async fn post(url: &str, body: &str) -> Result<(), String> {
    let client = pumasi_proto::registry::http_client().map_err(|e| format!("{e:#}"))?;
    let mut last = String::new();
    for attempt in 0..=RETRY_WAITS.len() {
        if attempt > 0 {
            tokio::time::sleep(RETRY_WAITS[attempt - 1]).await;
        }
        let res = client
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        match res {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => {
                let status = r.status();
                last = format!("HTTP {status}");
                // The receiver refused the request itself: trying again will
                // not help (except when it asks to slow down).
                if status.is_client_error() && status.as_u16() != 429 {
                    break;
                }
            }
            // Leave the URL out: its path is the webhook's secret.
            Err(e) => last = format!("{}", e.without_url()),
        }
    }
    Err(last)
}

/// Used and total bytes of the disk holding `dir`.
pub(crate) fn disk_of(dir: &Path) -> Option<(u64, u64)> {
    let dir = dir
        .canonicalize()
        .map(|d| pumasi_proto::without_verbatim(&d))
        .unwrap_or_else(|_| dir.to_path_buf());
    let disks = sysinfo::Disks::new_with_refreshed_list();
    disks
        .iter()
        .filter(|d| dir.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| {
            (
                d.total_space().saturating_sub(d.available_space()),
                d.total_space(),
            )
        })
}
