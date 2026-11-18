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

/// Writes one framed message with an optional tail after it in the same
/// frame. A reader that does not know the tail decodes the message and ignores
/// the rest, so a tail adds data without changing the protocol version.
pub async fn write_msg_tail<W, T, U>(w: &mut W, msg: &T, tail: Option<&U>) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
    U: Serialize,
{
    let mut bytes = postcard::to_stdvec(msg).context("encode message")?;
    if let Some(tail) = tail {
        bytes.extend(postcard::to_stdvec(tail).context("encode message tail")?);
    }
    if bytes.len() > MAX_FRAME {
        bail!("message too large: {} bytes (max {MAX_FRAME})", bytes.len());
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Reads one framed message and the tail after it, if the frame has one (see
/// [`write_msg_tail`]). A tail that does not decode is dropped: it is extra
/// data, never a reason to refuse the message.
pub async fn read_msg_tail<R, T, U>(r: &mut R) -> Result<Option<(T, Option<U>)>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    let Some(buf) = read_frame(r).await? else {
        return Ok(None);
    };
    let (msg, rest) = postcard::take_from_bytes::<T>(&buf).context("decode message")?;
    let tail = if rest.is_empty() {
        None
    } else {
        postcard::from_bytes::<U>(rest).ok()
    };
    Ok(Some((msg, tail)))
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

/// Deserializes a field added at the end of a struct that is the last
/// thing in its frame or record (1.22): bytes from an older peer simply
/// end before it, which postcard reports as an error, so the field takes
/// its default instead. Use with `#[serde(default, deserialize_with =
/// "crate::wire::trailing")]`; `default` covers JSON, where the key is
/// missing. Only for trailing fields: a field after it would read from
/// wherever the failed one left off.
pub fn trailing<'de, D, T>(d: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    Ok(T::deserialize(d).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailing_fields_default_when_the_bytes_end() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct Old {
            a: u32,
            b: Option<String>,
        }
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct New {
            a: u32,
            b: Option<String>,
            #[serde(default, deserialize_with = "trailing")]
            c: Option<u64>,
            #[serde(default, deserialize_with = "trailing")]
            d: Vec<u8>,
        }
        let old = postcard::to_stdvec(&Old {
            a: 7,
            b: Some("x".into()),
        })
        .unwrap();
        let new: New = postcard::from_bytes(&old).unwrap();
        assert_eq!(
            new,
            New {
                a: 7,
                b: Some("x".into()),
                c: None,
                d: vec![]
            }
        );
        // An older reader ignores what follows.
        let bytes = postcard::to_stdvec(&New {
            a: 1,
            b: None,
            c: Some(9),
            d: vec![1, 2],
        })
        .unwrap();
        let back: New = postcard::from_bytes(&bytes).unwrap();
        assert_eq!((back.c, back.d), (Some(9), vec![1, 2]));
        assert_eq!(
            postcard::from_bytes::<Old>(&bytes).unwrap(),
            Old { a: 1, b: None }
        );
        // JSON without the keys.
        let j: New = serde_json::from_str(r#"{"a":3,"b":null}"#).unwrap();
        assert_eq!((j.c, j.d.len()), (None, 0));
    }

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
    async fn tail_is_optional_on_both_sides() {
        // A reader that knows the tail gets it.
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_msg_tail(&mut a, &7u32, Some(&("tail".to_string(), 9u64)))
            .await
            .unwrap();
        let got: (u32, Option<(String, u64)>) = read_msg_tail(&mut b).await.unwrap().unwrap();
        assert_eq!(got, (7, Some(("tail".to_string(), 9))));

        // An older reader decodes the message and ignores the rest.
        write_msg_tail(&mut a, &7u32, Some(&("tail".to_string(), 9u64)))
            .await
            .unwrap();
        let got: u32 = read_msg(&mut b).await.unwrap().unwrap();
        assert_eq!(got, 7);

        // An older writer sends no tail.
        write_msg(&mut a, &7u32).await.unwrap();
        let got: (u32, Option<(String, u64)>) = read_msg_tail(&mut b).await.unwrap().unwrap();
        assert_eq!(got, (7, None));

        // No tail asked for is the same frame as `write_msg`.
        write_msg_tail(&mut a, &7u32, None::<&u8>).await.unwrap();
        let got: (u32, Option<(String, u64)>) = read_msg_tail(&mut b).await.unwrap().unwrap();
        assert_eq!(got, (7, None));

        // A tail that is not what the reader expects is dropped.
        write_msg_tail(&mut a, &7u32, Some(&1u8)).await.unwrap();
        let got: (u32, Option<(String, u64)>) = read_msg_tail(&mut b).await.unwrap().unwrap();
        assert_eq!(got, (7, None));
    }

    #[tokio::test]
    async fn rejects_oversized_frame() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&(u32::MAX).to_be_bytes()).await.unwrap();
        let res: Result<Option<u8>> = read_msg(&mut b).await;
        assert!(res.is_err());
    }
}
