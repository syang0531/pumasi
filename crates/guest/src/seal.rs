//! Sealed data for confidential tasks (phase 16).
//!
//! A confidential task runs in a microVM whose memory the host cannot read
//! (AMD SEV-SNP or Intel TDX). Everything the host hands it is sealed with
//! keys only the control plane and an attested guest know:
//!
//! - **Sealed streams.** The image's layers, the task's input disk and the
//!   guest's output are each one stream: a 64-byte header, then chunks of
//!   [`CHUNK`] plaintext bytes sealed with XChaCha20-Poly1305 (the STREAM
//!   construction: each chunk's nonce is a random prefix, its number and
//!   whether it is the last, so chunks cannot be reordered, dropped or
//!   cut off without the guest noticing). The header's kind is part of
//!   every chunk's associated data, so a layers stream cannot pass for an
//!   input one.
//! - **Keys.** The control plane keeps one random key per job. The image
//!   key and each task's key are derived from it ([`image_key`],
//!   [`task_key`]); outputs are sealed with the task key and bound to the
//!   run's nonce, so a host cannot hand in an older run's output.
//! - **Key release.** The guest makes an X25519 key pair and asks its TEE
//!   for a report whose report data is [`report_data`] of the run's nonce
//!   and its public key. The control plane checks the report and seals the
//!   two keys to that public key ([`wrap_keys`]); only the guest that made
//!   the report can open them ([`unwrap_keys`]).
//!
//! The attest disk carries the one round trip between the guest and the
//! host while the VM runs ([`AttestRequest`], [`Evidence`], [`AttestReply`]).

use std::io::{self, Read, Seek, SeekFrom, Write};

use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, Payload},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};

/// Plaintext bytes per sealed chunk.
pub const CHUNK: usize = 64 * 1024;
/// Poly1305 tag added to every chunk.
pub const TAG: usize = 16;
/// Bytes before the first chunk.
pub const HEADER: usize = 64;
/// First bytes of a sealed stream.
pub const SEALED_MAGIC: &[u8; 8] = b"PUMASEAL";
const VERSION: u8 = 1;
const PREFIX: usize = 19;

/// What a sealed stream holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// The image's layers: a tar in the format of the layers disk.
    Layers = 1,
    /// The task's input disk: a tar of the run spec, stdin and `/input`.
    Input = 2,
    /// The guest's result: a tar in the format of the output disk.
    Output = 3,
}

impl Kind {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Kind::Layers),
            2 => Some(Kind::Input),
            3 => Some(Kind::Output),
            _ => None,
        }
    }
}

/// The kind of the sealed stream starting with `head`, if it is one.
pub fn sealed_kind(head: &[u8]) -> Option<Kind> {
    if head.len() < HEADER || &head[..8] != SEALED_MAGIC || head[8] != VERSION {
        return None;
    }
    Kind::from_byte(head[9])
}

/// Bytes a sealed stream of `len` plaintext bytes takes.
pub fn sealed_len(len: u64) -> u64 {
    HEADER as u64 + len + TAG as u64 * chunks(len)
}

fn chunks(len: u64) -> u64 {
    len.div_ceil(CHUNK as u64).max(1)
}

/// Fills `buf` with random bytes from the operating system.
pub fn random(buf: &mut [u8]) {
    getrandom::fill(buf).expect("the operating system has no random numbers");
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn chunk_nonce(prefix: &[u8; PREFIX], index: u64, last: bool) -> io::Result<XNonce> {
    let index = u32::try_from(index).map_err(|_| invalid("sealed stream too long"))?;
    let mut n = [0u8; 24];
    n[..PREFIX].copy_from_slice(prefix);
    n[PREFIX..PREFIX + 4].copy_from_slice(&index.to_be_bytes());
    n[23] = last as u8;
    Ok(XNonce::from(n))
}

/// The associated data of every chunk: the header without its length
/// (written last), then the caller's binding.
fn chunk_ad(header: &[u8; HEADER], extra: &[u8]) -> Vec<u8> {
    let mut ad = header.to_vec();
    ad[16..24].fill(0);
    ad.extend_from_slice(extra);
    ad
}

/// Seals what is written to it. Call [`SealWriter::finish`] at the end:
/// it seals the last chunk and writes the length into the header.
pub struct SealWriter<W: Write + Seek> {
    out: W,
    start: u64,
    cipher: XChaCha20Poly1305,
    prefix: [u8; PREFIX],
    ad: Vec<u8>,
    buf: Vec<u8>,
    index: u64,
    len: u64,
}

impl<W: Write + Seek> SealWriter<W> {
    /// Starts a stream of `kind` at the current position of `out`.
    /// `extra` is bound to every chunk (the run's nonce for outputs).
    pub fn new(mut out: W, key: &[u8; 32], kind: Kind, extra: &[u8]) -> io::Result<Self> {
        let start = out.stream_position()?;
        let mut prefix = [0u8; PREFIX];
        random(&mut prefix);
        let mut header = [0u8; HEADER];
        header[..8].copy_from_slice(SEALED_MAGIC);
        header[8] = VERSION;
        header[9] = kind as u8;
        header[24..24 + PREFIX].copy_from_slice(&prefix);
        out.write_all(&header)?;
        Ok(Self {
            out,
            start,
            cipher: XChaCha20Poly1305::new(key.into()),
            prefix,
            ad: chunk_ad(&header, extra),
            buf: Vec::with_capacity(CHUNK),
            index: 0,
            len: 0,
        })
    }

    fn seal_chunk(&mut self, last: bool) -> io::Result<()> {
        let nonce = chunk_nonce(&self.prefix, self.index, last)?;
        let sealed = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &self.buf,
                    aad: &self.ad,
                },
            )
            .map_err(|_| io::Error::other("seal failed"))?;
        self.out.write_all(&sealed)?;
        self.buf.clear();
        self.index += 1;
        Ok(())
    }

    /// Seals the last chunk, writes the length into the header and returns
    /// the writer (positioned after the stream) and the stream's size.
    pub fn finish(mut self) -> io::Result<(W, u64)> {
        self.seal_chunk(true)?;
        let end = self.out.stream_position()?;
        self.out.seek(SeekFrom::Start(self.start + 16))?;
        self.out.write_all(&self.len.to_le_bytes())?;
        self.out.seek(SeekFrom::Start(end))?;
        self.out.flush()?;
        Ok((self.out, end - self.start))
    }
}

impl<W: Write + Seek> Write for SealWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        // A full chunk is sealed only once more data comes: until then it
        // may be the last.
        if self.buf.len() == CHUNK {
            self.seal_chunk(false)?;
        }
        let n = data.len().min(CHUNK - self.buf.len());
        self.buf.extend_from_slice(&data[..n]);
        self.len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

/// Opens a sealed stream, chunk by chunk. A chunk that does not open (a
/// wrong key, kind or binding, or bytes changed, reordered or cut off) is
/// an `InvalidData` error.
pub struct SealReader<R: Read> {
    inner: R,
    cipher: XChaCha20Poly1305,
    prefix: [u8; PREFIX],
    ad: Vec<u8>,
    len: u64,
    chunks: u64,
    index: u64,
    plain: Vec<u8>,
    pos: usize,
}

impl<R: Read> SealReader<R> {
    pub fn new(mut inner: R, key: &[u8; 32], kind: Kind, extra: &[u8]) -> io::Result<Self> {
        let mut header = [0u8; HEADER];
        inner.read_exact(&mut header)?;
        match sealed_kind(&header) {
            Some(k) if k == kind => {}
            Some(k) => return Err(invalid(format!("a sealed {k:?} stream, not {kind:?}"))),
            None => return Err(invalid("not a sealed stream")),
        }
        let len = u64::from_le_bytes(header[16..24].try_into().expect("8 bytes"));
        let mut prefix = [0u8; PREFIX];
        prefix.copy_from_slice(&header[24..24 + PREFIX]);
        Ok(Self {
            inner,
            cipher: XChaCha20Poly1305::new(key.into()),
            prefix,
            ad: chunk_ad(&header, extra),
            len,
            chunks: chunks(len),
            index: 0,
            plain: Vec::new(),
            pos: 0,
        })
    }

    /// Plaintext bytes in the stream, as its header says.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn next_chunk(&mut self) -> io::Result<bool> {
        if self.index == self.chunks {
            return Ok(false);
        }
        let last = self.index + 1 == self.chunks;
        let plain = if last {
            self.len - (self.chunks - 1) * CHUNK as u64
        } else {
            CHUNK as u64
        } as usize;
        let mut sealed = vec![0u8; plain + TAG];
        self.inner.read_exact(&mut sealed).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                invalid("sealed stream cut off")
            } else {
                e
            }
        })?;
        let nonce = chunk_nonce(&self.prefix, self.index, last)?;
        self.plain = self
            .cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &sealed,
                    aad: &self.ad,
                },
            )
            .map_err(|_| invalid("sealed data does not open (changed, or the wrong key)"))?;
        self.pos = 0;
        self.index += 1;
        Ok(true)
    }
}

impl<R: Read> Read for SealReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.pos == self.plain.len() {
            if !self.next_chunk()? {
                return Ok(0);
            }
        }
        let n = out.len().min(self.plain.len() - self.pos);
        out[..n].copy_from_slice(&self.plain[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Seals all of `src` into `dst`. Returns the sealed size.
pub fn seal_all<R: Read, W: Write + Seek>(
    src: &mut R,
    dst: W,
    key: &[u8; 32],
    kind: Kind,
    extra: &[u8],
) -> io::Result<u64> {
    let mut w = SealWriter::new(dst, key, kind, extra)?;
    io::copy(src, &mut w)?;
    Ok(w.finish()?.1)
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

fn hkdf(salt: &[u8], ikm: &[u8], info: &[&[u8]]) -> [u8; 32] {
    let h = hkdf::Hkdf::<Sha256>::new(Some(salt), ikm);
    let info: Vec<u8> = info.concat();
    let mut out = [0u8; 32];
    h.expand(&info, &mut out)
        .expect("32 bytes is a valid length");
    out
}

/// The key the job's image layers are sealed with.
pub fn image_key(job_key: &[u8; 32]) -> [u8; 32] {
    hkdf(b"pumasi/seal/v1", job_key, &[b"image"])
}

/// The key a task's input and its runs' outputs are sealed with.
pub fn task_key(job_key: &[u8; 32], index: u32) -> [u8; 32] {
    hkdf(b"pumasi/seal/v1", job_key, &[b"task", &index.to_be_bytes()])
}

/// The keys an attested guest gets.
#[derive(Clone, PartialEq, Eq)]
pub struct TaskKeys {
    pub image: [u8; 32],
    pub task: [u8; 32],
}

impl std::fmt::Debug for TaskKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TaskKeys(..)")
    }
}

/// What the guest asks its TEE to put in the report: the run's nonce and
/// the public key the keys will be sealed to.
pub fn report_data(nonce: &[u8; 32], guest_key: &[u8; 32]) -> [u8; 64] {
    let mut h = Sha512::new();
    h.update(b"pumasi/attest/v1\0");
    h.update(nonce);
    h.update(guest_key);
    h.finalize().into()
}

/// A new X25519 key pair: (secret, public).
pub fn key_pair() -> ([u8; 32], [u8; 32]) {
    let mut secret = [0u8; 32];
    random(&mut secret);
    let public = x25519_dalek::x25519(secret, x25519_dalek::X25519_BASEPOINT_BYTES);
    (secret, public)
}

const WRAP_VERSION: u8 = 1;

fn wrap_key(shared: &[u8; 32], nonce: &[u8; 32], eph: &[u8; 32], guest: &[u8; 32]) -> [u8; 32] {
    hkdf(nonce, shared, &[b"pumasi/keywrap/v1", eph, guest])
}

/// Seals `keys` to the guest's public key for the run with `nonce`.
pub fn wrap_keys(guest_key: &[u8; 32], nonce: &[u8; 32], keys: &TaskKeys) -> io::Result<Vec<u8>> {
    let (eph_secret, eph) = key_pair();
    let shared = x25519_dalek::x25519(eph_secret, *guest_key);
    if shared == [0u8; 32] {
        return Err(invalid("the guest's key is not a valid X25519 key"));
    }
    let key = wrap_key(&shared, nonce, &eph, guest_key);
    let mut xnonce = [0u8; 24];
    random(&mut xnonce);
    let mut plain = Vec::with_capacity(64);
    plain.extend_from_slice(&keys.image);
    plain.extend_from_slice(&keys.task);
    let sealed = XChaCha20Poly1305::new((&key).into())
        .encrypt(&XNonce::from(xnonce), plain.as_slice())
        .map_err(|_| io::Error::other("seal failed"))?;
    let mut out = vec![WRAP_VERSION];
    out.extend_from_slice(&eph);
    out.extend_from_slice(&xnonce);
    out.extend_from_slice(&sealed);
    Ok(out)
}

/// Opens what [`wrap_keys`] sealed, with the guest's secret key.
pub fn unwrap_keys(secret: &[u8; 32], nonce: &[u8; 32], wrapped: &[u8]) -> io::Result<TaskKeys> {
    if wrapped.len() != 1 + 32 + 24 + 64 + TAG || wrapped[0] != WRAP_VERSION {
        return Err(invalid("not a wrapped key"));
    }
    let eph: [u8; 32] = wrapped[1..33].try_into().expect("32 bytes");
    let xnonce: [u8; 24] = wrapped[33..57].try_into().expect("24 bytes");
    let public = x25519_dalek::x25519(*secret, x25519_dalek::X25519_BASEPOINT_BYTES);
    let shared = x25519_dalek::x25519(*secret, eph);
    let key = wrap_key(&shared, nonce, &eph, &public);
    let plain = XChaCha20Poly1305::new((&key).into())
        .decrypt(&XNonce::from(xnonce), &wrapped[57..])
        .map_err(|_| invalid("the wrapped key does not open"))?;
    Ok(TaskKeys {
        image: plain[..32].try_into().expect("32 bytes"),
        task: plain[32..64].try_into().expect("32 bytes"),
    })
}

// ---------------------------------------------------------------------------
// The attest disk
// ---------------------------------------------------------------------------

/// Size of the attest disk.
pub const ATTEST_DISK_BYTES: u64 = 1 << 20;
/// Where the guest writes its evidence, and the most it may take.
pub const EVIDENCE_AT: u64 = 4096;
pub const EVIDENCE_MAX: usize = 256 * 1024;
/// Where the host writes its reply, and the most it may take.
pub const REPLY_AT: u64 = 512 * 1024;
pub const REPLY_MAX: usize = 4096;
const REQUEST_MAGIC: &[u8; 8] = b"PUMATTRQ";
const EVIDENCE_MAGIC: &[u8; 8] = b"PUMATTEV";
const REPLY_MAGIC: &[u8; 8] = b"PUMATTKY";

/// Which TEE the guest is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TeeKind {
    /// AMD SEV-SNP.
    SevSnp,
    /// Intel TDX.
    Tdx,
    /// No TEE: a stand-in for tests, which a control plane accepts only
    /// when started with `--insecure-test-tee`.
    Test,
}

impl std::fmt::Display for TeeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TeeKind::SevSnp => "sev-snp",
            TeeKind::Tdx => "tdx",
            TeeKind::Test => "test",
        })
    }
}

impl std::str::FromStr for TeeKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "sev-snp" | "snp" => Ok(TeeKind::SevSnp),
            "tdx" => Ok(TeeKind::Tdx),
            "test" => Ok(TeeKind::Test),
            other => Err(format!("unknown TEE {other:?} (sev-snp, tdx)")),
        }
    }
}

impl TeeKind {
    fn byte(self) -> u8 {
        match self {
            TeeKind::SevSnp => 1,
            TeeKind::Tdx => 2,
            TeeKind::Test => 3,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(TeeKind::SevSnp),
            2 => Some(TeeKind::Tdx),
            3 => Some(TeeKind::Test),
            _ => None,
        }
    }
}

/// What the host writes at the start of the attest disk before the VM
/// boots: the run's nonce and the TEE the guest should be in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestRequest {
    pub tee: TeeKind,
    pub nonce: [u8; 32],
}

impl AttestRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = REQUEST_MAGIC.to_vec();
        out.push(1);
        out.push(self.tee.byte());
        out.extend_from_slice(&[0; 6]);
        out.extend_from_slice(&self.nonce);
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < 48 || &b[..8] != REQUEST_MAGIC || b[8] != 1 {
            return None;
        }
        Some(Self {
            tee: TeeKind::from_byte(b[9])?,
            nonce: b[16..48].try_into().ok()?,
        })
    }
}

/// The guest's evidence: what its TEE reported, and the public key the
/// report data binds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub tee: TeeKind,
    /// SEV-SNP: the attestation report. TDX: the quote. Test: empty.
    pub report: Vec<u8>,
    /// SEV-SNP: the certificate table the host gave with an extended
    /// report (VCEK, ASK, ARK), if any.
    #[serde(default)]
    pub aux: Vec<u8>,
    pub key: [u8; 32],
    /// Test only: the report data the guest would have asked for.
    #[serde(default)]
    pub report_data: Vec<u8>,
}

/// Frames a message for a region of the attest disk: magic, length,
/// bytes, padded to 4 KiB (the guest writes with `O_DIRECT`).
fn frame(magic: &[u8; 8], body: &[u8]) -> Vec<u8> {
    let mut out = magic.to_vec();
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    out.resize(out.len().div_ceil(4096) * 4096, 0);
    out
}

/// The body of a framed message, if `b` starts with one that is complete.
fn unframe<'a>(magic: &[u8; 8], b: &'a [u8], max: usize) -> Option<&'a [u8]> {
    if b.len() < 12 || &b[..8] != magic {
        return None;
    }
    let n = u32::from_le_bytes(b[8..12].try_into().ok()?) as usize;
    if n > max || b.len() < 12 + n {
        return None;
    }
    Some(&b[12..12 + n])
}

impl Evidence {
    pub fn frame(&self) -> Vec<u8> {
        frame(
            EVIDENCE_MAGIC,
            &serde_json::to_vec(self).expect("evidence serializes"),
        )
    }

    /// The evidence at the start of `b` (the disk from [`EVIDENCE_AT`]),
    /// once the guest wrote all of it.
    pub fn unframe(b: &[u8]) -> Option<Result<Self, String>> {
        let body = unframe(EVIDENCE_MAGIC, b, EVIDENCE_MAX)?;
        Some(serde_json::from_slice(body).map_err(|e| format!("bad evidence: {e}")))
    }
}

/// The host's answer: the keys sealed to the guest, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestReply {
    Keys(Vec<u8>),
    Refused(String),
}

impl AttestReply {
    pub fn frame(&self) -> Vec<u8> {
        let mut body = Vec::new();
        match self {
            AttestReply::Keys(k) => {
                body.push(0);
                body.extend_from_slice(k);
            }
            AttestReply::Refused(why) => {
                body.push(1);
                let why = why.as_bytes();
                body.extend_from_slice(&why[..why.len().min(REPLY_MAX - 32)]);
            }
        }
        frame(REPLY_MAGIC, &body)
    }

    pub fn unframe(b: &[u8]) -> Option<Self> {
        let body = unframe(REPLY_MAGIC, b, REPLY_MAX)?;
        let (&tag, rest) = body.split_first()?;
        Some(match tag {
            0 => AttestReply::Keys(rest.to_vec()),
            _ => AttestReply::Refused(String::from_utf8_lossy(rest).into_owned()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn seal(data: &[u8], key: &[u8; 32], kind: Kind, extra: &[u8]) -> Vec<u8> {
        let mut out = Cursor::new(Vec::new());
        let n = seal_all(&mut &data[..], &mut out, key, kind, extra).unwrap();
        let out = out.into_inner();
        assert_eq!(n as usize, out.len());
        assert_eq!(out.len() as u64, sealed_len(data.len() as u64));
        out
    }

    fn open(sealed: &[u8], key: &[u8; 32], kind: Kind, extra: &[u8]) -> io::Result<Vec<u8>> {
        let mut r = SealReader::new(sealed, key, kind, extra)?;
        let mut out = Vec::new();
        r.read_to_end(&mut out)?;
        Ok(out)
    }

    #[test]
    fn streams_round_trip_at_every_chunk_boundary() {
        let key = [7u8; 32];
        for len in [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK, 3 * CHUNK + 5] {
            let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let sealed = seal(&data, &key, Kind::Input, b"");
            assert_eq!(sealed_kind(&sealed), Some(Kind::Input));
            assert_eq!(
                open(&sealed, &key, Kind::Input, b"").unwrap(),
                data,
                "len {len}"
            );
        }
    }

    #[test]
    fn changed_cut_or_rebound_streams_do_not_open() {
        let key = [7u8; 32];
        let data = vec![5u8; 2 * CHUNK + 100];
        let sealed = seal(&data, &key, Kind::Output, b"nonce");
        // Wrong key, kind or binding.
        assert!(open(&sealed, &[8u8; 32], Kind::Output, b"nonce").is_err());
        assert!(open(&sealed, &key, Kind::Input, b"nonce").is_err());
        assert!(open(&sealed, &key, Kind::Output, b"other").is_err());
        // A flipped bit anywhere after the header.
        for at in [HEADER, HEADER + CHUNK + 3, sealed.len() - 1] {
            let mut bad = sealed.clone();
            bad[at] ^= 1;
            assert!(
                open(&bad, &key, Kind::Output, b"nonce").is_err(),
                "flip at {at}"
            );
        }
        // The kind byte changed to pass for another stream.
        let mut bad = sealed.clone();
        bad[9] = Kind::Input as u8;
        assert!(open(&bad, &key, Kind::Input, b"nonce").is_err());
        // Cut off after whole chunks, with the length made to match.
        let mut cut = sealed[..HEADER + CHUNK + TAG].to_vec();
        cut[16..24].copy_from_slice(&(CHUNK as u64).to_le_bytes());
        assert!(open(&cut, &key, Kind::Output, b"nonce").is_err());
        // Cut off without fixing the length.
        assert!(open(&sealed[..sealed.len() - 10], &key, Kind::Output, b"nonce").is_err());
        // Two chunks swapped.
        let mut swapped = sealed.clone();
        let (a, b) = (HEADER, HEADER + CHUNK + TAG);
        let first = sealed[a..b].to_vec();
        swapped[a..b].copy_from_slice(&sealed[b..b + CHUNK + TAG]);
        swapped[b..b + CHUNK + TAG].copy_from_slice(&first);
        assert!(open(&swapped, &key, Kind::Output, b"nonce").is_err());
    }

    #[test]
    fn keys_reach_only_the_guest_that_asked() {
        let (secret, public) = key_pair();
        let nonce = [3u8; 32];
        let job = [9u8; 32];
        let keys = TaskKeys {
            image: image_key(&job),
            task: task_key(&job, 4),
        };
        assert_ne!(keys.image, keys.task);
        assert_ne!(task_key(&job, 4), task_key(&job, 5));
        let wrapped = wrap_keys(&public, &nonce, &keys).unwrap();
        assert_eq!(unwrap_keys(&secret, &nonce, &wrapped).unwrap(), keys);
        // Another key pair, another nonce, or a changed byte: nothing.
        let (other, _) = key_pair();
        assert!(unwrap_keys(&other, &nonce, &wrapped).is_err());
        assert!(unwrap_keys(&secret, &[4u8; 32], &wrapped).is_err());
        let mut bad = wrapped.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(unwrap_keys(&secret, &nonce, &bad).is_err());
        // A low-order point is refused rather than sealed to.
        assert!(wrap_keys(&[0u8; 32], &nonce, &keys).is_err());
        // The report data binds both the nonce and the key.
        assert_ne!(
            report_data(&nonce, &public),
            report_data(&[4u8; 32], &public)
        );
        assert_ne!(
            report_data(&nonce, &public),
            report_data(&nonce, &[1u8; 32])
        );
    }

    #[test]
    fn attest_disk_messages_round_trip() {
        let req = AttestRequest {
            tee: TeeKind::SevSnp,
            nonce: [1u8; 32],
        };
        assert_eq!(AttestRequest::decode(&req.encode()), Some(req));
        let ev = Evidence {
            tee: TeeKind::Tdx,
            report: vec![1, 2, 3],
            aux: vec![],
            key: [2u8; 32],
            report_data: vec![],
        };
        let framed = ev.frame();
        assert_eq!(framed.len() % 4096, 0);
        assert_eq!(Evidence::unframe(&framed), Some(Ok(ev)));
        // Not written yet, or only partly.
        assert_eq!(Evidence::unframe(&[0u8; 4096]), None);
        assert_eq!(Evidence::unframe(&framed[..20]), None);
        for reply in [
            AttestReply::Keys(vec![9; 137]),
            AttestReply::Refused("unknown measurement".into()),
        ] {
            assert_eq!(AttestReply::unframe(&reply.frame()), Some(reply));
        }
    }
}
