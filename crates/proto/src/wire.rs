//! Length-prefixed postcard framing over any async byte stream.
//!
//! Each frame is a 4-byte big-endian length followed by a postcard-encoded
//! value. Frames larger than [`MAX_FRAME`] are rejected on both sides.

use anyhow::{Context, Result, bail};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest frame accepted (64 MiB): bounds module uploads and outputs.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Writes one framed message.
pub async fn write_msg<W, T>(w: &mut W, msg: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = postcard::to_stdvec(msg).context("encode message")?;
    if bytes.len() > MAX_FRAME {
        bail!("message too large: {} bytes (max {MAX_FRAME})", bytes.len());
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Reads one framed message. Returns `Ok(None)` on a clean end of stream.
pub async fn read_msg<R, T>(r: &mut R) -> Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    match read_frame(r).await? {
        Some(buf) => Ok(Some(postcard::from_bytes(&buf).context("decode message")?)),
        None => Ok(None),
    }
}

/// Reads one frame's bytes, for messages decoded with a fallback (see
/// `AgentHello::decode`). `Ok(None)` on a clean end of stream.
pub async fn read_frame<R>(r: &mut R) -> Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        bail!("frame too large: {len} bytes (max {MAX_FRAME})");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await.context("read frame body")?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_msg(&mut a, &("hello".to_string(), 42u64))
            .await
            .unwrap();
        drop(a);
        let got: (String, u64) = read_msg(&mut b).await.unwrap().unwrap();
        assert_eq!(got, ("hello".to_string(), 42));
        let end: Option<(String, u64)> = read_msg(&mut b).await.unwrap();
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn rejects_oversized_frame() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&(u32::MAX).to_be_bytes()).await.unwrap();
        let res: Result<Option<u8>> = read_msg(&mut b).await;
        assert!(res.is_err());
    }
}
