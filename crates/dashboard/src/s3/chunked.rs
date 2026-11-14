//! `aws-chunked` bodies: the `STREAMING-*` payloads SDKs send for uploads,
//! where each chunk carries its own signature (chained from the request's)
//! and checksums may follow in trailers.

use sha2::{Digest, Sha256};

use super::sigv4::{Bad, EMPTY_SHA256};

/// Longest chunk header or trailer line.
const MAX_LINE: usize = 4096;
/// Most trailer bytes.
const MAX_TRAILER: usize = 16 * 1024;

/// What chunk signatures are checked with.
pub struct ChunkSigner {
    pub key: [u8; 32],
    pub amz_date: String,
    pub scope: String,
    /// The request's signature, then each chunk's.
    pub prev: String,
}

enum St {
    /// Reading a chunk's header line.
    Header,
    /// Reading a chunk's data.
    Data {
        left: u64,
        sig: Option<String>,
    },
    /// The CRLF after a chunk's data.
    DataEnd,
    /// Trailer lines after the last chunk.
    Trailer,
    Done,
}

pub struct Chunked {
    buf: Vec<u8>,
    st: St,
    signer: Option<ChunkSigner>,
    trailer: bool,
    hash: Sha256,
    trailers: Vec<(String, String)>,
    trailer_bytes: usize,
}

fn bad(msg: impl Into<String>) -> Bad {
    Bad("IncompleteBody", msg.into())
}

impl Chunked {
    /// `signer`: check chunk signatures; `trailer`: trailers follow the
    /// last chunk.
    pub fn new(signer: Option<ChunkSigner>, trailer: bool) -> Self {
        Self {
            buf: Vec::new(),
            st: St::Header,
            signer,
            trailer,
            hash: Sha256::new(),
            trailers: Vec::new(),
            trailer_bytes: 0,
        }
    }

    /// Decodes more of the body, appending its data to `out`.
    pub fn feed(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<(), Bad> {
        loop {
            match &mut self.st {
                St::Data { left, .. } if *left > 0 => {
                    if input.is_empty() {
                        return Ok(());
                    }
                    let n = (*left).min(input.len() as u64) as usize;
                    let (d, rest) = input.split_at(n);
                    if self.signer.is_some() {
                        self.hash.update(d);
                    }
                    out.extend_from_slice(d);
                    *left -= n as u64;
                    input = rest;
                }
                St::Data { sig, .. } => {
                    let sig = sig.take();
                    let hash = std::mem::take(&mut self.hash).finalize();
                    self.check_chunk(sig, &data_encoding::HEXLOWER.encode(&hash))?;
                    self.st = St::DataEnd;
                }
                St::Done => {
                    if input.iter().any(|b| !b.is_ascii_whitespace()) {
                        return Err(bad("data after the last chunk"));
                    }
                    return Ok(());
                }
                _ => {
                    // Line-based states.
                    let Some(line) = self.line(&mut input)? else {
                        return Ok(());
                    };
                    self.on_line(line)?;
                }
            }
        }
    }

    /// The next CRLF-terminated line, buffering partial input.
    fn line(&mut self, input: &mut &[u8]) -> Result<Option<String>, Bad> {
        if let Some(i) = input.iter().position(|&b| b == b'\n') {
            self.buf.extend_from_slice(&input[..i]);
            *input = &input[i + 1..];
            let mut l = std::mem::take(&mut self.buf);
            if l.last() == Some(&b'\r') {
                l.pop();
            }
            return String::from_utf8(l)
                .map(Some)
                .map_err(|_| bad("a chunk header is not text"));
        }
        self.buf.extend_from_slice(input);
        *input = &[];
        if self.buf.len() > MAX_LINE {
            return Err(bad("a chunk header is too long"));
        }
        Ok(None)
    }

    fn on_line(&mut self, line: String) -> Result<(), Bad> {
        match self.st {
            St::DataEnd => {
                if !line.is_empty() {
                    return Err(bad("a chunk is longer than its size"));
                }
                self.st = St::Header;
            }
            St::Header => {
                let (size, ext) = match line.split_once(';') {
                    Some((s, e)) => (s, Some(e)),
                    None => (line.as_str(), None),
                };
                let size = u64::from_str_radix(size.trim(), 16)
                    .map_err(|_| bad(format!("bad chunk size {size:?}")))?;
                let sig = ext
                    .and_then(|e| e.trim().strip_prefix("chunk-signature="))
                    .map(str::to_string);
                if size > 0 {
                    self.st = St::Data { left: size, sig };
                    return Ok(());
                }
                // The last chunk: signed like any other, with no data.
                self.check_chunk(sig, EMPTY_SHA256)?;
                // Trailer lines (when announced), then an empty line.
                self.st = St::Trailer;
            }
            St::Trailer => {
                self.trailer_bytes += line.len();
                if self.trailer_bytes > MAX_TRAILER {
                    return Err(bad("trailers are too long"));
                }
                if line.is_empty() {
                    self.check_trailer_signature()?;
                    self.st = St::Done;
                    return Ok(());
                }
                if !self.trailer {
                    return Err(bad("unexpected data after the last chunk"));
                }
                let (k, v) = line
                    .split_once(':')
                    .ok_or_else(|| bad("a malformed trailer"))?;
                self.trailers
                    .push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
            }
            St::Data { .. } | St::Done => unreachable!(),
        }
        Ok(())
    }

    fn check_chunk(&mut self, sig: Option<String>, hash: &str) -> Result<(), Bad> {
        let Some(s) = &mut self.signer else {
            return Ok(());
        };
        let sig = sig.ok_or_else(|| Bad("SignatureDoesNotMatch", "a chunk is unsigned".into()))?;
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{EMPTY_SHA256}\n{hash}",
            s.amz_date, s.scope, s.prev
        );
        if !pumasi_proto::s3::same(&pumasi_proto::s3::sign(&s.key, &sts), &sig) {
            return Err(Bad(
                "SignatureDoesNotMatch",
                "a chunk's signature does not match".into(),
            ));
        }
        s.prev = sig;
        Ok(())
    }

    /// With signed chunks, the trailers end with their own signature.
    fn check_trailer_signature(&mut self) -> Result<(), Bad> {
        let Some(s) = &self.signer else {
            return Ok(());
        };
        let Some(pos) = self
            .trailers
            .iter()
            .position(|(k, _)| k == "x-amz-trailer-signature")
        else {
            if self.trailers.is_empty() {
                return Ok(());
            }
            return Err(Bad(
                "SignatureDoesNotMatch",
                "the trailers are unsigned".into(),
            ));
        };
        let (_, sig) = self.trailers.remove(pos);
        let canonical: String = self
            .trailers
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect();
        let sts = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
            s.amz_date,
            s.scope,
            s.prev,
            pumasi_proto::s3::sha256_hex(canonical.as_bytes())
        );
        if !pumasi_proto::s3::same(&pumasi_proto::s3::sign(&s.key, &sts), &sig) {
            return Err(Bad(
                "SignatureDoesNotMatch",
                "the trailers' signature does not match".into(),
            ));
        }
        Ok(())
    }

    /// The trailers, once the body is complete.
    pub fn finish(self) -> Result<Vec<(String, String)>, Bad> {
        match self.st {
            St::Done => Ok(self.trailers),
            _ => Err(bad("the body ended before its last chunk")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Trailers = Vec<(String, String)>;

    fn decode(
        body: &[u8],
        signer: Option<ChunkSigner>,
        trailer: bool,
        step: usize,
    ) -> Result<(Vec<u8>, Trailers), Bad> {
        let mut c = Chunked::new(signer, trailer);
        let mut out = Vec::new();
        for piece in body.chunks(step) {
            c.feed(piece, &mut out)?;
        }
        Ok((out, c.finish()?))
    }

    /// The example of "Signature Calculations for the Authorization Header:
    /// Transferring Payload in Multiple Chunks" in the S3 documentation.
    #[test]
    fn documentation_example() {
        let key = pumasi_proto::s3::signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );
        let signer = || ChunkSigner {
            key,
            amz_date: "20130524T000000Z".into(),
            scope: "20130524/us-east-1/s3/aws4_request".into(),
            prev: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9".into(),
        };
        let mut body = Vec::new();
        body.extend_from_slice(b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n");
        body.extend(std::iter::repeat_n(b'a', 65536));
        body.extend_from_slice(b"\r\n400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n");
        body.extend(std::iter::repeat_n(b'a', 1024));
        body.extend_from_slice(b"\r\n0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n");
        for step in [1, 7, 1000, body.len()] {
            let (data, trailers) = decode(&body, Some(signer()), false, step).unwrap();
            assert_eq!(data.len(), 66560);
            assert!(trailers.is_empty());
        }
        // One changed byte breaks the chain.
        let mut bad_body = body.clone();
        bad_body[200] = b'b';
        assert!(decode(&bad_body, Some(signer()), false, 4096).is_err());
        // Cut short.
        assert!(decode(&body[..body.len() - 10], Some(signer()), false, 4096).is_err());
    }

    #[test]
    fn unsigned_with_trailer() {
        let body = b"5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n";
        let (data, trailers) = decode(body, None, true, 3).unwrap();
        assert_eq!(data, b"hello world");
        assert_eq!(
            trailers,
            [("x-amz-checksum-crc32".to_string(), "DUoRhQ==".to_string())]
        );
    }
}
