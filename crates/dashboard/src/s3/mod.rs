//! The S3-compatible gateway (1.8) at `/s3` of `pumasi ui`: buckets are
//! datasets, objects are the files of a dataset's latest version, so
//! `aws s3`, rclone and boto3 read and write the cluster file store.
//!
//! Requests are signed with AWS Signature Version 4 by keys made from
//! access tickets (`pumasi s3 create-key`). The control plane holds the
//! keys' secrets: the gateway builds the string to sign and asks it to
//! check the signature ([`AdminOp::S3Auth`]), getting back a credential
//! that acts as the key's ticket for a few minutes and the day's signing
//! key, with which it checks the key's next requests (and the chunk
//! signatures of streamed uploads) itself until the credential runs out.
//!
//! Every write makes a new version of the dataset (S3 buckets have no
//! versions, so it replaces the version the gateway's previous write made;
//! see [`pumasi_proto::msg::AdminOp::S3Commit`]). Writes to one bucket
//! that arrive while one is being committed are committed together. The
//! gateway keeps the bodies of uploads, and the parts of objects it read,
//! in a file store of its own in the temporary directory.

mod chunked;
mod sigv4;
mod xml;

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::any,
};
use md5::{Digest as _, Md5};
use n0_future::StreamExt;
use pumasi_proto::{
    Secret, blobs,
    manage::AccessRole,
    msg::{AdminOp, AdminResponse},
    replica::{MAX_S3_META_BYTES, S3Meta},
    s3 as sig,
    store::BlobHash,
};
use sha2::Sha256;
use tokio::sync::oneshot;

use crate::{AbortOnDrop, St};

use sigv4::{Bad, Signed};
use xml::esc;

/// How long the gateway checks a key's requests itself before asking the
/// control plane again (its credential lasts 5 minutes).
const CRED_TTL: Duration = Duration::from_secs(240);
/// How long a bucket's file list is reused for reads.
const LISTING_TTL: Duration = Duration::from_secs(2);
/// How long the gateway keeps what it read of an object.
const READ_CACHE: Duration = Duration::from_secs(600);
/// Multipart uploads not finished within this are dropped.
const UPLOAD_EXPIRY: Duration = Duration::from_secs(24 * 3600);
const MAX_UPLOADS: usize = 1000;
const MAX_PARTS: u32 = 10_000;
/// Largest XML body (completing an upload, deleting objects).
const MAX_XML: u64 = 4 << 20;
const MAX_KEYS: usize = 1000;
const MAX_COMMIT_TRIES: u32 = 6;

/// Query parameters of S3 features the gateway does not offer.
const UNSUPPORTED: &[&str] = &[
    "acl",
    "tagging",
    "policy",
    "policyStatus",
    "cors",
    "lifecycle",
    "encryption",
    "replication",
    "website",
    "logging",
    "notification",
    "object-lock",
    "retention",
    "legal-hold",
    "ownershipControls",
    "publicAccessBlock",
    "accelerate",
    "requestPayment",
    "analytics",
    "inventory",
    "metrics",
    "intelligent-tiering",
    "torrent",
    "restore",
    "select",
    "attributes",
    "versions",
    "versionId",
];

pub fn routes() -> Router<St> {
    Router::new()
        .route("/s3", any(handle))
        .route("/s3/", any(handle))
        .route("/s3/{*rest}", any(handle))
}

// -- errors ------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct S3Error {
    code: &'static str,
    msg: String,
}

fn err(code: &'static str, msg: impl Into<String>) -> S3Error {
    S3Error {
        code,
        msg: msg.into(),
    }
}

fn internal(e: impl std::fmt::Display) -> S3Error {
    err("InternalError", e.to_string())
}

impl S3Error {
    fn status(&self) -> StatusCode {
        match self.code {
            "NoSuchBucket" | "NoSuchKey" | "NoSuchUpload" => StatusCode::NOT_FOUND,
            "AccessDenied"
            | "InvalidAccessKeyId"
            | "SignatureDoesNotMatch"
            | "RequestTimeTooSkewed"
            | "QuotaExceeded" => StatusCode::FORBIDDEN,
            "BucketNotEmpty" | "BucketAlreadyOwnedByYou" | "OperationAborted" => {
                StatusCode::CONFLICT
            }
            "MethodNotAllowed" => StatusCode::METHOD_NOT_ALLOWED,
            "PreconditionFailed" => StatusCode::PRECONDITION_FAILED,
            "InvalidRange" => StatusCode::RANGE_NOT_SATISFIABLE,
            "NotImplemented" => StatusCode::NOT_IMPLEMENTED,
            "InternalError" => StatusCode::INTERNAL_SERVER_ERROR,
            "ServiceUnavailable" | "SlowDown" => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<Bad> for S3Error {
    fn from(Bad(code, msg): Bad) -> Self {
        err(code, msg)
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>",
            self.code,
            esc(&self.msg)
        );
        (
            self.status(),
            [(header::CONTENT_TYPE, "application/xml")],
            body,
        )
            .into_response()
    }
}

/// What a control plane refusal means to an S3 client.
fn from_cp(e: anyhow::Error) -> S3Error {
    let msg = format!("{e:#}");
    let code = if msg.contains("no dataset") {
        "NoSuchBucket"
    } else if msg.contains("cannot reach the control plane")
        || msg.contains("no control plane replica is the leader")
    {
        "ServiceUnavailable"
    } else if msg.contains("invalid admin credentials") {
        "InvalidAccessKeyId"
    } else if msg.contains("ticket cannot")
        || msg.contains("belongs to")
        || msg.contains("has versions of account")
        || msg.contains("this ticket submits for account")
    {
        "AccessDenied"
    } else if msg.contains("quota") {
        "QuotaExceeded"
    } else if msg.contains("Conflict") {
        "OperationAborted"
    } else if msg.contains("dataset name") {
        "InvalidBucketName"
    } else if msg.contains("path") || msg.contains("metadata") {
        "InvalidArgument"
    } else {
        "InternalError"
    };
    err(code, msg)
}

// -- state -------------------------------------------------------------------

#[derive(Clone)]
struct Cached {
    token: Secret,
    signing_key: [u8; 32],
    name: String,
    role: AccessRole,
    until: Instant,
}

/// The caller of one request.
#[derive(Clone)]
struct Cred {
    key: String,
    token: Secret,
    signing_key: [u8; 32],
    name: String,
    role: AccessRole,
    signed: Signed,
}

/// A bucket's objects: the files of the dataset's latest version.
struct Listing {
    name: String,
    version: u32,
    created_unix: u64,
    /// By key.
    objects: Vec<Obj>,
}

struct Obj {
    key: String,
    /// Its place in the collection.
    index: u64,
    size: u64,
    hash: BlobHash,
    meta: Option<S3Meta>,
}

impl Listing {
    fn get(&self, key: &str) -> Option<&Obj> {
        self.objects
            .binary_search_by(|o| o.key.as_str().cmp(key))
            .ok()
            .map(|i| &self.objects[i])
    }

    fn modified(&self, o: &Obj) -> u64 {
        o.meta
            .as_ref()
            .map_or(self.created_unix, |m| m.modified_unix)
    }
}

impl Obj {
    /// The MD5 the gateway recorded when it wrote the object; otherwise
    /// part of its BLAKE3 hash, shaped like a multipart ETag so that
    /// clients do not take it for an MD5.
    fn etag(&self) -> String {
        match &self.meta {
            Some(m) => m.etag.clone(),
            None => format!("{}-1", data_encoding::HEXLOWER.encode(&self.hash[..16])),
        }
    }
}

type Change = (String, Option<(blobs::Hash, S3Meta)>);
type Listed = (Instant, Arc<Listing>);

struct Waiter {
    changes: Vec<Change>,
    done: oneshot::Sender<Result<u32, S3Error>>,
}

#[derive(Default)]
struct Queue {
    pending: Vec<Waiter>,
    cred: Option<Cred>,
}

struct Part {
    _tag: blobs::TempTag,
    hash: blobs::Hash,
    size: u64,
    md5: [u8; 16],
    at_unix: u64,
}

struct Upload {
    bucket: String,
    key: String,
    /// The access key that started it.
    owner: String,
    started: Instant,
    started_unix: u64,
    content_type: Option<String>,
    meta: Vec<(String, String)>,
    parts: BTreeMap<u32, Part>,
}

/// The gateway's own file store.
struct Cache {
    store: blobs::FsStore,
    dir: PathBuf,
    /// Objects being read, or read lately (users, last use); each has a
    /// tag that keeps it from garbage collection.
    reading: Arc<Mutex<HashMap<blobs::Hash, (usize, Instant)>>>,
    _sweeper: AbortOnDrop,
}

impl Drop for Cache {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Keeps a blob from garbage collection while it is read.
struct Hold {
    reading: Arc<Mutex<HashMap<blobs::Hash, (usize, Instant)>>>,
    hash: blobs::Hash,
}

impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(e) = self.reading.lock().unwrap().get_mut(&self.hash) {
            e.0 = e.0.saturating_sub(1);
            e.1 = Instant::now();
        }
    }
}

fn read_tag(hash: &blobs::Hash) -> String {
    format!("s3/{}", hash.to_hex())
}

impl Cache {
    async fn open() -> anyhow::Result<Self> {
        sweep_stale_dirs();
        let dir = std::env::temp_dir().join(format!(
            "pumasi-ui-s3-{}-{:08x}",
            std::process::id(),
            rand::random::<u32>()
        ));
        let mut b = std::fs::DirBuilder::new();
        b.recursive(true);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
        b.create(&dir)?;
        let store = blobs::open_store(&dir, Some((Duration::from_secs(120), None))).await?;
        let reading: Arc<Mutex<HashMap<blobs::Hash, (usize, Instant)>>> = Default::default();
        let sweeper = {
            let store = store.clone();
            let reading = reading.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    let stale: Vec<blobs::Hash> = {
                        let mut r = reading.lock().unwrap();
                        let stale: Vec<_> = r
                            .iter()
                            .filter(|(_, (n, last))| *n == 0 && last.elapsed() > READ_CACHE)
                            .map(|(h, _)| *h)
                            .collect();
                        for h in &stale {
                            r.remove(h);
                        }
                        stale
                    };
                    for h in &stale {
                        let _ = store.tags().delete(read_tag(h)).await;
                    }
                    // Read again meanwhile: tag it again.
                    let back: Vec<blobs::Hash> = {
                        let r = reading.lock().unwrap();
                        stale.into_iter().filter(|h| r.contains_key(h)).collect()
                    };
                    for h in back {
                        let _ = store
                            .tags()
                            .set(read_tag(&h), blobs::HashAndFormat::raw(h))
                            .await;
                    }
                }
            })
        };
        Ok(Self {
            store,
            dir,
            reading,
            _sweeper: AbortOnDrop(sweeper),
        })
    }

    async fn hold(&self, hash: blobs::Hash) -> Hold {
        let new = {
            let mut r = self.reading.lock().unwrap();
            let e = r.entry(hash).or_insert((0, Instant::now()));
            e.0 += 1;
            e.1 = Instant::now();
            e.0 == 1
        };
        if new {
            let _ = self
                .store
                .tags()
                .set(read_tag(&hash), blobs::HashAndFormat::raw(hash))
                .await;
        }
        Hold {
            reading: self.reading.clone(),
            hash,
        }
    }
}

/// Removes the stores of gateways that are no longer running.
fn sweep_stale_dirs() {
    #[cfg(target_os = "linux")]
    if let Ok(rd) = std::fs::read_dir(std::env::temp_dir()) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix("pumasi-ui-s3-") else {
                continue;
            };
            let Some(pid) = rest.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
                continue;
            };
            if pid != std::process::id() && !std::path::Path::new(&format!("/proc/{pid}")).exists()
            {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    }
}

#[derive(Default)]
pub struct Gateway {
    creds: Mutex<HashMap<(String, String, String), Cached>>,
    /// By (access key, bucket), with when it was read.
    listings: Mutex<HashMap<(String, String), Listed>>,
    queues: Mutex<HashMap<(String, String), Queue>>,
    uploads: Mutex<HashMap<String, Upload>>,
    cache: tokio::sync::OnceCell<Cache>,
}

impl Gateway {
    async fn cache(&self) -> Result<&Cache, S3Error> {
        self.cache
            .get_or_try_init(Cache::open)
            .await
            .map_err(|e| internal(format!("open the gateway's file store: {e:#}")))
    }

    fn forget_listings(&self, bucket: &str) {
        self.listings
            .lock()
            .unwrap()
            .retain(|(_, b), _| b != bucket);
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn http_date(t: u64) -> String {
    chrono::DateTime::from_timestamp(t as i64, 0)
        .unwrap_or_default()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

fn iso_date(t: u64) -> String {
    chrono::DateTime::from_timestamp(t as i64, 0)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string()
}

fn hdr<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/xml")],
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"),
    )
        .into_response()
}

// -- authentication ----------------------------------------------------------

async fn authenticate(st: &St, signed: Signed) -> Result<Cred, S3Error> {
    let ck = (
        signed.key.clone(),
        signed.date.clone(),
        signed.region.clone(),
    );
    let cached = st
        .s3
        .creds
        .lock()
        .unwrap()
        .get(&ck)
        .filter(|c| c.until > Instant::now())
        .cloned();
    if let Some(c) = cached {
        sig::check_string_to_sign(
            &signed.string_to_sign,
            &signed.date,
            &signed.region,
            now_unix(),
            signed.expires,
        )
        .map_err(|e| auth_error(&e))?;
        if !sig::same(
            &sig::sign(&c.signing_key, &signed.string_to_sign),
            &signed.signature,
        ) {
            return Err(err(
                "SignatureDoesNotMatch",
                "the request signature does not match (check the secret key)",
            ));
        }
        return Ok(Cred {
            key: signed.key.clone(),
            token: c.token,
            signing_key: c.signing_key,
            name: c.name,
            role: c.role,
            signed,
        });
    }
    let resp = st
        .backend
        .call(
            st.backend.own_secret(),
            AdminOp::S3Auth {
                key: signed.key.clone(),
                string_to_sign: signed.string_to_sign.clone(),
                signature: signed.signature.clone(),
                expires: signed.expires,
            },
        )
        .await;
    match resp {
        Ok(AdminResponse::S3Auth {
            token,
            signing_key,
            name,
            role,
        }) => {
            st.s3.creds.lock().unwrap().insert(
                ck,
                Cached {
                    token,
                    signing_key,
                    name: name.clone(),
                    role: role.clone(),
                    until: Instant::now() + CRED_TTL,
                },
            );
            Ok(Cred {
                key: signed.key.clone(),
                token,
                signing_key,
                name,
                role,
                signed,
            })
        }
        Ok(_) => Err(internal("unexpected response")),
        Err(e) => {
            let msg = format!("{e:#}");
            if msg.contains("cannot reach the control plane") {
                Err(err("ServiceUnavailable", msg))
            } else {
                Err(auth_error(&msg))
            }
        }
    }
}

fn auth_error(msg: &str) -> S3Error {
    if msg.contains("InvalidAccessKeyId") {
        err(
            "InvalidAccessKeyId",
            "the access key does not exist (or was revoked)",
        )
    } else if msg.contains("SignatureDoesNotMatch") {
        err(
            "SignatureDoesNotMatch",
            "the request signature does not match (check the secret key)",
        )
    } else if msg.contains("RequestTimeTooSkewed") {
        err(
            "RequestTimeTooSkewed",
            "the request time is more than 15 minutes from the server's",
        )
    } else {
        err("AccessDenied", msg.to_string())
    }
}

/// An admin call as the request's caller, signing in again once when
/// the control plane no longer knows the credential (a new leader).
async fn call(st: &St, cred: &mut Cred, op: AdminOp) -> Result<AdminResponse, S3Error> {
    match st.backend.call(cred.token, op.clone()).await {
        Err(e) if format!("{e:#}").contains("invalid admin credentials") => {
            st.s3.creds.lock().unwrap().remove(&(
                cred.key.clone(),
                cred.signed.date.clone(),
                cred.signed.region.clone(),
            ));
            *cred = authenticate(st, cred.signed.clone()).await?;
            st.backend.call(cred.token, op).await.map_err(from_cp)
        }
        r => r.map_err(from_cp),
    }
}

// -- request bodies ------------------------------------------------------------

/// A request body as it arrives: decoded from `aws-chunked`, hashed, and
/// checked against what the request says it is once it is complete.
struct Payload {
    stream: axum::body::BodyDataStream,
    chunked: Option<chunked::Chunked>,
    md5: Md5,
    sha: Option<Sha256>,
    want_sha: Option<String>,
    crc: Option<crc32fast::Hasher>,
    /// Checksums from the headers, by header name.
    checksums: Vec<(String, String)>,
    n: u64,
    limit: u64,
    decoded_len: Option<u64>,
    content_md5: Option<String>,
}

impl Payload {
    fn new(body: Body, headers: &HeaderMap, cred: &Cred, limit: u64) -> Result<Self, S3Error> {
        let s = &cred.signed;
        let signer = || chunked::ChunkSigner {
            key: cred.signing_key,
            amz_date: s.amz_date.clone(),
            scope: s.scope(),
            prev: s.signature.clone(),
        };
        let mut want_sha = None;
        let chunked = match s.payload.as_str() {
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => {
                Some(chunked::Chunked::new(Some(signer()), false))
            }
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => {
                Some(chunked::Chunked::new(Some(signer()), true))
            }
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Some(chunked::Chunked::new(None, true)),
            "UNSIGNED-PAYLOAD" => None,
            p if p.starts_with("STREAMING-") => {
                return Err(err(
                    "NotImplemented",
                    format!("payload signing {p} is not supported"),
                ));
            }
            p if p.len() == 64 && p.bytes().all(|b| b.is_ascii_hexdigit()) => {
                want_sha = Some(p.to_ascii_lowercase());
                None
            }
            p => {
                return Err(err(
                    "InvalidArgument",
                    format!("x-amz-content-sha256 {p:?} is not a SHA-256"),
                ));
            }
        };
        let mut checksums = Vec::new();
        for (k, v) in headers {
            let k = k.as_str();
            if k.starts_with("x-amz-checksum-")
                && k != "x-amz-checksum-type"
                && k != "x-amz-checksum-algorithm"
                && let Ok(v) = v.to_str()
            {
                checksums.push((k.to_string(), v.trim().to_string()));
            }
        }
        let trailer = hdr(headers, "x-amz-trailer")
            .unwrap_or("")
            .to_ascii_lowercase();
        let wants = |name: &str| {
            trailer.split(',').any(|t| t.trim() == name) || checksums.iter().any(|(k, _)| k == name)
        };
        let sha = (want_sha.is_some() || wants("x-amz-checksum-sha256")).then(Sha256::new);
        let crc = wants("x-amz-checksum-crc32").then(crc32fast::Hasher::new);
        Ok(Self {
            stream: body.into_data_stream(),
            chunked,
            md5: Md5::new(),
            sha,
            want_sha,
            crc,
            checksums,
            n: 0,
            limit,
            decoded_len: hdr(headers, "x-amz-decoded-content-length").and_then(|v| v.parse().ok()),
            content_md5: hdr(headers, "content-md5").map(str::to_string),
        })
    }

    async fn next(&mut self) -> Result<Option<Bytes>, S3Error> {
        loop {
            let Some(frame) = self.stream.next().await else {
                return Ok(None);
            };
            let frame = frame.map_err(|e| err("IncompleteBody", format!("read the body: {e}")))?;
            let data = match &mut self.chunked {
                Some(c) => {
                    let mut out = Vec::new();
                    c.feed(&frame, &mut out)?;
                    Bytes::from(out)
                }
                None => frame,
            };
            if data.is_empty() {
                continue;
            }
            self.n += data.len() as u64;
            if self.n > self.limit {
                return Err(err(
                    "EntityTooLarge",
                    format!(
                        "the body is larger than this gateway's upload limit ({} bytes)",
                        self.limit
                    ),
                ));
            }
            self.md5.update(&data);
            if let Some(s) = &mut self.sha {
                sha2::Digest::update(s, &data);
            }
            if let Some(c) = &mut self.crc {
                c.update(&data);
            }
            return Ok(Some(data));
        }
    }

    /// Checks the whole body; returns its MD5.
    fn finish(self) -> Result<[u8; 16], S3Error> {
        let mut checksums = self.checksums;
        if let Some(c) = self.chunked {
            checksums.extend(
                c.finish()?
                    .into_iter()
                    .filter(|(k, _)| k.starts_with("x-amz-checksum-")),
            );
        }
        if let Some(d) = self.decoded_len
            && d != self.n
        {
            return Err(err(
                "IncompleteBody",
                format!("the body has {} bytes, not {d}", self.n),
            ));
        }
        let md5: [u8; 16] = self.md5.finalize().into();
        let sha: Option<[u8; 32]> = self.sha.map(|s| s.finalize().into());
        if let (Some(want), Some(sha)) = (&self.want_sha, &sha)
            && *want != data_encoding::HEXLOWER.encode(sha)
        {
            return Err(err(
                "XAmzContentSHA256Mismatch",
                "the body does not match x-amz-content-sha256",
            ));
        }
        if let Some(m) = &self.content_md5
            && data_encoding::BASE64.encode(&md5) != m.trim()
        {
            return Err(err("BadDigest", "the body does not match Content-MD5"));
        }
        let crc = self.crc.map(|c| c.finalize());
        for (k, v) in &checksums {
            let got = match k.as_str() {
                "x-amz-checksum-crc32" => {
                    crc.map(|c| data_encoding::BASE64.encode(&c.to_be_bytes()))
                }
                "x-amz-checksum-sha256" => sha.map(|s| data_encoding::BASE64.encode(&s)),
                // Other checksums (CRC32C, CRC64NVME, SHA-1) go unchecked.
                _ => None,
            };
            if let Some(got) = got
                && got != *v
            {
                return Err(err("BadDigest", format!("the body does not match {k}")));
            }
        }
        Ok(md5)
    }

    /// The whole body, for small XML requests.
    async fn read_all(mut self) -> Result<Vec<u8>, S3Error> {
        let mut out = Vec::new();
        while let Some(b) = self.next().await? {
            out.extend_from_slice(&b);
        }
        self.finish()?;
        Ok(out)
    }
}

/// Imports a body into the gateway's store: (its blob, kept while the
/// tag lives; size; MD5).
async fn store_body(
    cache: &Cache,
    mut p: Payload,
) -> Result<(blobs::TempTag, u64, [u8; 16]), S3Error> {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(8);
    let stream =
        n0_future::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|x| (x, rx)) });
    let import = async {
        cache
            .store
            .blobs()
            .add_stream(stream)
            .await
            .temp_tag()
            .await
    };
    let feed = async move {
        loop {
            match p.next().await {
                Ok(Some(b)) => {
                    if tx.send(Ok(b)).await.is_err() {
                        return Err(internal("the store stopped taking the body"));
                    }
                }
                Ok(None) => return Ok(p),
                Err(e) => {
                    let _ = tx.send(Err(std::io::Error::other("aborted"))).await;
                    return Err(e);
                }
            }
        }
    };
    let (tag, fed) = tokio::join!(import, feed);
    let p = fed?;
    let tag = tag.map_err(|e| internal(format!("store the body: {e}")))?;
    let n = p.n;
    let md5 = p.finish()?;
    Ok((tag, n, md5))
}

fn user_meta(headers: &HeaderMap) -> Result<Vec<(String, String)>, S3Error> {
    let mut out: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| {
            let name = k.as_str().strip_prefix("x-amz-meta-")?;
            Some((name.to_string(), v.to_str().ok()?.to_string()))
        })
        .collect();
    out.sort();
    let bytes: usize = out.iter().map(|(k, v)| k.len() + v.len()).sum();
    if bytes > MAX_S3_META_BYTES {
        return Err(err(
            "MetadataTooLarge",
            format!("user metadata is limited to {MAX_S3_META_BYTES} bytes"),
        ));
    }
    Ok(out)
}

// -- dispatch ------------------------------------------------------------------

async fn handle(
    State(st): State<St>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let head = method == Method::HEAD;
    let mut resp = match serve(&st, &method, &uri, &headers, body).await {
        Ok(r) => r,
        Err(e) => {
            if e.code == "InternalError" || e.code == "ServiceUnavailable" {
                tracing::warn!(code = e.code, "s3 {method} {}: {}", uri.path(), e.msg);
            }
            e.into_response()
        }
    };
    if head {
        let len = resp.headers().get(header::CONTENT_LENGTH).cloned();
        *resp.body_mut() = Body::empty();
        if let Some(len) = len {
            resp.headers_mut().insert(header::CONTENT_LENGTH, len);
        }
    }
    let h = resp.headers_mut();
    h.insert(
        "x-amz-request-id",
        HeaderValue::from_str(&format!("{:016X}", rand::random::<u64>())).unwrap(),
    );
    h.insert(header::SERVER, HeaderValue::from_static("pumasi"));
    resp
}

async fn serve(
    st: &St,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let path = uri.path();
    let rest = path.strip_prefix("/s3").unwrap_or(path);
    let rest = rest.strip_prefix('/').unwrap_or(rest);
    let (bucket, key) = match rest.split_once('/') {
        Some((b, k)) => (sigv4::decode(b), sigv4::decode(k)),
        None => (sigv4::decode(rest), String::new()),
    };
    let q = sigv4::query_pairs(uri);
    let has = |k: &str| q.iter().any(|(n, _)| n == k);
    let qv = |k: &str| q.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    let signed = sigv4::read(method, uri, headers)?.ok_or_else(|| {
        err(
            "AccessDenied",
            "anonymous requests are not accepted: sign them with a key from `pumasi s3 create-key`",
        )
    })?;
    let mut cred = authenticate(st, signed).await?;
    let writes = !matches!(*method, Method::GET | Method::HEAD);
    if writes && cred.role == AccessRole::Viewer {
        return Err(err(
            "AccessDenied",
            "this key's ticket is a viewer ticket, which cannot write",
        ));
    }
    if bucket.is_empty() {
        return match *method {
            Method::GET => list_buckets(st, &mut cred).await,
            _ => Err(err("MethodNotAllowed", "only GET lists buckets")),
        };
    }
    if let Some((k, _)) = q.iter().find(|(k, _)| UNSUPPORTED.contains(&k.as_str())) {
        return Err(err(
            "NotImplemented",
            format!("?{k} is not supported by this gateway"),
        ));
    }
    if key.is_empty() {
        return match *method {
            Method::HEAD => {
                listing(st, &mut cred, &bucket, false)
                    .await?
                    .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
                Ok(StatusCode::OK.into_response())
            }
            Method::GET if has("location") => {
                listing(st, &mut cred, &bucket, false)
                    .await?
                    .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
                Ok(xml_response(
                    StatusCode::OK,
                    format!("<LocationConstraint xmlns=\"{}\"/>", xml::NS),
                ))
            }
            Method::GET if has("versioning") => Ok(xml_response(
                StatusCode::OK,
                format!("<VersioningConfiguration xmlns=\"{}\"/>", xml::NS),
            )),
            Method::GET if has("uploads") => list_uploads(st, &cred, &bucket),
            Method::GET => {
                list_objects(st, &mut cred, &bucket, &q, qv("list-type") == Some("2")).await
            }
            Method::PUT => create_bucket(st, &mut cred, &bucket, body, headers).await,
            Method::DELETE => delete_bucket(st, &mut cred, &bucket).await,
            Method::POST if has("delete") => {
                delete_objects(st, &mut cred, &bucket, body, headers).await
            }
            _ => Err(err("MethodNotAllowed", "not a bucket operation")),
        };
    }
    match *method {
        Method::GET if has("uploadId") => {
            list_parts(st, &cred, &bucket, &key, qv("uploadId").unwrap_or(""))
        }
        Method::GET | Method::HEAD => {
            get_object(
                st,
                &mut cred,
                &bucket,
                &key,
                headers,
                &q,
                *method == Method::HEAD,
            )
            .await
        }
        Method::PUT if has("uploadId") => {
            if headers.contains_key("x-amz-copy-source") {
                return Err(err("NotImplemented", "UploadPartCopy is not supported"));
            }
            upload_part(st, &cred, &bucket, &key, &q, body, headers).await
        }
        Method::PUT if headers.contains_key("x-amz-copy-source") => {
            copy_object(st, &mut cred, &bucket, &key, headers).await
        }
        Method::PUT => put_object(st, &mut cred, &bucket, &key, body, headers).await,
        Method::DELETE if has("uploadId") => {
            abort_upload(st, &cred, &bucket, &key, qv("uploadId").unwrap_or(""))
        }
        Method::DELETE => delete_object(st, &mut cred, &bucket, &key).await,
        Method::POST if has("uploads") => create_upload(st, &cred, &bucket, &key, headers),
        Method::POST if has("uploadId") => {
            complete_upload(
                st,
                &mut cred,
                &bucket,
                &key,
                qv("uploadId").unwrap_or(""),
                body,
                headers,
            )
            .await
        }
        _ => Err(err("MethodNotAllowed", "not an object operation")),
    }
}

fn check_key(key: &str) -> Result<(), S3Error> {
    if key.len() > 1024 {
        return Err(err("KeyTooLongError", "keys are limited to 1024 bytes"));
    }
    pumasi_proto::job::validate_rel_path(key).map_err(|e| {
        err(
            "InvalidArgument",
            format!("key {key:?} is not a path a dataset can hold: {e:#}"),
        )
    })
}

// -- reading -------------------------------------------------------------------

/// A bucket's objects; `None` when there is no such dataset.
async fn listing(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    fresh: bool,
) -> Result<Option<Arc<Listing>>, S3Error> {
    let ck = (cred.key.clone(), bucket.to_string());
    if !fresh
        && let Some((at, l)) = st.s3.listings.lock().unwrap().get(&ck)
        && at.elapsed() < LISTING_TTL
    {
        return Ok(Some(l.clone()));
    }
    let resp = call(
        st,
        cred,
        AdminOp::StoreFiles {
            name: bucket.to_string(),
            version: None,
        },
    )
    .await;
    let (dataset, files, s3) = match resp {
        Ok(AdminResponse::StoreFiles { dataset, files, s3 }) => (dataset, files, s3),
        Ok(_) => return Err(internal("unexpected response")),
        Err(e) if e.code == "NoSuchBucket" => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut meta: HashMap<String, S3Meta> = s3.into_iter().collect();
    let mut objects: Vec<Obj> = files
        .into_iter()
        .enumerate()
        .map(|(i, f)| Obj {
            meta: meta.remove(&f.path),
            key: f.path,
            index: i as u64,
            size: f.size,
            hash: f.hash,
        })
        .collect();
    objects.sort_by(|a, b| a.key.cmp(&b.key));
    let l = Arc::new(Listing {
        name: dataset.name,
        version: dataset.version,
        created_unix: dataset.created_unix,
        objects,
    });
    st.s3
        .listings
        .lock()
        .unwrap()
        .insert(ck, (Instant::now(), l.clone()));
    Ok(Some(l))
}

async fn list_buckets(st: &St, cred: &mut Cred) -> Result<Response, S3Error> {
    let AdminResponse::Datasets(ds) = call(st, cred, AdminOp::StoreList { name: None }).await?
    else {
        return Err(internal("unexpected response"));
    };
    let mut buckets: BTreeMap<String, u64> = BTreeMap::new();
    for d in ds {
        let e = buckets.entry(d.name).or_insert(d.created_unix);
        *e = (*e).min(d.created_unix);
    }
    let mut x = format!(
        "<ListAllMyBucketsResult xmlns=\"{}\"><Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner><Buckets>",
        xml::NS,
        esc(&cred.name),
        esc(&cred.name)
    );
    for (name, created) in buckets {
        x.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            esc(&name),
            iso_date(created)
        ));
    }
    x.push_str("</Buckets></ListAllMyBucketsResult>");
    Ok(xml_response(StatusCode::OK, x))
}

async fn list_objects(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    q: &[(String, String)],
    v2: bool,
) -> Result<Response, S3Error> {
    let qv = |k: &str| q.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    let l = listing(st, cred, bucket, false)
        .await?
        .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
    let prefix = qv("prefix").unwrap_or("");
    let delimiter = qv("delimiter").unwrap_or("");
    let max_keys = match qv("max-keys") {
        Some(m) => m
            .parse::<usize>()
            .map_err(|_| err("InvalidArgument", "max-keys must be a number"))?
            .min(MAX_KEYS),
        None => MAX_KEYS,
    };
    let url = qv("encoding-type") == Some("url");
    let enc = |s: &str| {
        if url { sigv4::encode(s, true) } else { esc(s) }
    };
    let token = match qv("continuation-token") {
        Some(t) if v2 => Some(
            data_encoding::BASE64URL_NOPAD
                .decode(t.as_bytes())
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| err("InvalidArgument", "the continuation token is not valid"))?,
        ),
        _ => None,
    };
    let start = if v2 {
        token
            .clone()
            .or_else(|| qv("start-after").map(str::to_string))
            .unwrap_or_default()
    } else {
        qv("marker").unwrap_or("").to_string()
    };
    let mut contents = String::new();
    let mut prefixes: Vec<String> = Vec::new();
    let mut count = 0usize;
    let mut last = String::new();
    let mut truncated = false;
    let from = l
        .objects
        .partition_point(|o| o.key.as_str() <= start.as_str());
    for o in &l.objects[from..] {
        if !o.key.starts_with(prefix) {
            if o.key.as_str() > prefix {
                break;
            }
            continue;
        }
        // Inside a folder already rolled up as the marker.
        if !delimiter.is_empty() && start.ends_with(delimiter) && o.key.starts_with(&start) {
            continue;
        }
        let common = (!delimiter.is_empty())
            .then(|| {
                o.key[prefix.len()..]
                    .find(delimiter)
                    .map(|i| o.key[..prefix.len() + i + delimiter.len()].to_string())
            })
            .flatten();
        if let Some(c) = &common
            && prefixes.last() == Some(c)
        {
            continue;
        }
        if count >= max_keys {
            truncated = true;
            break;
        }
        count += 1;
        match common {
            Some(c) => {
                last = c.clone();
                prefixes.push(c);
            }
            None => {
                last = o.key.clone();
                contents.push_str(&format!(
                    "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    enc(&o.key),
                    iso_date(l.modified(o)),
                    esc(&o.etag()),
                    o.size
                ));
            }
        }
    }
    let mut x = format!(
        "<ListBucketResult xmlns=\"{}\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
        xml::NS,
        esc(bucket),
        enc(prefix)
    );
    if !delimiter.is_empty() {
        x.push_str(&format!("<Delimiter>{}</Delimiter>", enc(delimiter)));
    }
    if url {
        x.push_str("<EncodingType>url</EncodingType>");
    }
    if v2 {
        x.push_str(&format!("<KeyCount>{count}</KeyCount>"));
        if let Some(t) = qv("continuation-token") {
            x.push_str(&format!(
                "<ContinuationToken>{}</ContinuationToken>",
                esc(t)
            ));
        }
        if let Some(s) = qv("start-after") {
            x.push_str(&format!("<StartAfter>{}</StartAfter>", enc(s)));
        }
        if truncated {
            x.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                data_encoding::BASE64URL_NOPAD.encode(last.as_bytes())
            ));
        }
    } else {
        x.push_str(&format!("<Marker>{}</Marker>", enc(&start)));
        if truncated {
            x.push_str(&format!("<NextMarker>{}</NextMarker>", enc(&last)));
        }
    }
    x.push_str(&contents);
    for p in prefixes {
        x.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            enc(&p)
        ));
    }
    x.push_str("</ListBucketResult>");
    Ok(xml_response(StatusCode::OK, x))
}

fn etag_matches(cond: &str, etag: &str) -> bool {
    cond.split(',')
        .map(|c| c.trim().trim_start_matches("W/").trim_matches('"'))
        .any(|c| c == "*" || c == etag)
}

/// A `Range` header as `start..end`; `None` for the whole object.
fn parse_range(h: Option<&str>, size: u64) -> Result<Option<(u64, u64)>, S3Error> {
    let Some(spec) = h.and_then(|h| h.trim().strip_prefix("bytes=")) else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None);
    }
    let Some((a, b)) = spec.split_once('-') else {
        return Ok(None);
    };
    let bad = || err("InvalidRange", "the requested range is not satisfiable");
    let (a, b) = (a.trim(), b.trim());
    let range = if a.is_empty() {
        let n: u64 = b.parse().map_err(|_| bad())?;
        if n == 0 || size == 0 {
            return Err(bad());
        }
        (size.saturating_sub(n), size)
    } else {
        let s: u64 = a.parse().map_err(|_| bad())?;
        let e = if b.is_empty() {
            size
        } else {
            let e: u64 = b.parse().map_err(|_| bad())?;
            if e < s {
                return Ok(None);
            }
            (e + 1).min(size)
        };
        if s >= size {
            return Err(bad());
        }
        (s, e)
    };
    Ok(Some(range))
}

fn header_value(s: &str) -> Option<HeaderValue> {
    HeaderValue::from_str(s).ok()
}

#[allow(clippy::too_many_arguments)]
async fn get_object(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    q: &[(String, String)],
    head: bool,
) -> Result<Response, S3Error> {
    let qv = |k: &str| q.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    let l = listing(st, cred, bucket, false)
        .await?
        .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
    let o = l
        .get(key)
        .ok_or_else(|| err("NoSuchKey", format!("no object {key} in {bucket}")))?;
    let etag = o.etag();
    let modified = l.modified(o);
    if let Some(m) = hdr(headers, "if-match")
        && !etag_matches(m, &etag)
    {
        return Err(err("PreconditionFailed", "If-Match does not match"));
    }
    let mut h = HeaderMap::new();
    h.insert(header::ETAG, header_value(&format!("\"{etag}\"")).unwrap());
    h.insert(
        header::LAST_MODIFIED,
        header_value(&http_date(modified)).unwrap(),
    );
    if let Some(m) = hdr(headers, "if-none-match")
        && etag_matches(m, &etag)
    {
        return Ok((StatusCode::NOT_MODIFIED, h).into_response());
    }
    let range = parse_range(hdr(headers, "range"), o.size)?;
    let (start, end) = range.unwrap_or((0, o.size));
    let content_type = qv("response-content-type")
        .map(str::to_string)
        .or_else(|| o.meta.as_ref().and_then(|m| m.content_type.clone()))
        .unwrap_or_else(|| "application/octet-stream".into());
    if let Some(v) = header_value(&content_type) {
        h.insert(header::CONTENT_TYPE, v);
    }
    for (q, name) in [
        ("response-content-disposition", header::CONTENT_DISPOSITION),
        ("response-cache-control", header::CACHE_CONTROL),
        ("response-content-encoding", header::CONTENT_ENCODING),
        ("response-content-language", header::CONTENT_LANGUAGE),
    ] {
        if let Some(v) = qv(q).and_then(header_value) {
            h.insert(name, v);
        }
    }
    if let Some(m) = &o.meta {
        for (k, v) in &m.meta {
            if let (Ok(name), Some(v)) = (
                axum::http::HeaderName::from_bytes(format!("x-amz-meta-{k}").as_bytes()),
                header_value(v),
            ) {
                h.insert(name, v);
            }
        }
    }
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(end - start));
    // Objects are other people's data: never let a browser run them on the
    // dashboard's origin.
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    let status = if range.is_some() {
        h.insert(
            header::CONTENT_RANGE,
            header_value(&format!(
                "bytes {start}-{}/{}",
                end.saturating_sub(1),
                o.size
            ))
            .unwrap(),
        );
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    if head || end == start {
        return Ok((status, h, Body::empty()).into_response());
    }
    let cache = st.s3.cache().await?;
    let hash = blobs::to_hash(&o.hash);
    let hold = cache.hold(hash).await;
    let complete = matches!(
        cache.store.blobs().status(hash).await,
        Ok(blobs::BlobStatus::Complete { .. })
    );
    if !complete {
        let client = st
            .backend
            .client(cred.token)
            .await
            .map_err(|e| err("ServiceUnavailable", format!("{e:#}")))?;
        let wanted = range.map(|(s, e)| s..e);
        client
            .fetch_dataset_files(&cache.store, &l.name, l.version, &[(o.index, wanted)])
            .await
            .map_err(from_cp)?;
    }
    let stream = cache
        .store
        .blobs()
        .export_ranges(hash, start..end)
        .stream()
        .filter_map(move |item| {
            let _ = &hold;
            clip(item, start, end)
        });
    Ok((status, h, Body::from_stream(stream)).into_response())
}

/// What an export yields of `start..end`.
fn clip(item: blobs::ExportRangesItem, start: u64, end: u64) -> Option<std::io::Result<Bytes>> {
    use blobs::ExportRangesItem as Item;
    match item {
        Item::Size(_) => None,
        Item::Data(leaf) => {
            let off = leaf.offset;
            let len = leaf.data.len() as u64;
            let s = start.max(off);
            let e = end.min(off + len);
            (s < e).then(|| Ok(leaf.data.slice((s - off) as usize..(e - off) as usize)))
        }
        Item::Error(e) => Some(Err(std::io::Error::other(e.to_string()))),
    }
}

// -- writing -------------------------------------------------------------------

/// Commits changes to a bucket (`None` removes a key), together with the
/// other writes to it that arrive meanwhile. Returns the new version.
async fn commit(st: &St, cred: &Cred, bucket: &str, changes: Vec<Change>) -> Result<u32, S3Error> {
    let (tx, rx) = oneshot::channel();
    let qk = (bucket.to_string(), cred.key.clone());
    let start = {
        let mut qs = st.s3.queues.lock().unwrap();
        let fresh = !qs.contains_key(&qk);
        let q = qs.entry(qk.clone()).or_default();
        q.pending.push(Waiter { changes, done: tx });
        q.cred = Some(cred.clone());
        fresh
    };
    if start {
        let st = st.clone();
        tokio::spawn(async move { committer(st, qk).await });
    }
    rx.await.map_err(|_| internal("the write was dropped"))?
}

async fn committer(st: St, qk: (String, String)) {
    loop {
        let (batch, cred) = {
            let mut qs = st.s3.queues.lock().unwrap();
            let Some(q) = qs.get_mut(&qk) else {
                return;
            };
            if q.pending.is_empty() {
                qs.remove(&qk);
                return;
            }
            let Some(cred) = q.cred.clone() else {
                qs.remove(&qk);
                return;
            };
            (std::mem::take(&mut q.pending), cred)
        };
        let results = commit_batch(&st, cred, &qk.0, &batch).await;
        for (w, r) in batch.into_iter().zip(results) {
            let _ = w.done.send(r);
        }
    }
}

/// Why a key cannot go into a dataset next to the others: a file tree
/// cannot have `a` both as a file and as a folder.
fn folder_clash(files: &BTreeMap<String, blobs::Hash>, key: &str) -> Result<(), S3Error> {
    for (i, _) in key.match_indices('/') {
        if files.contains_key(&key[..i]) {
            return Err(err(
                "InvalidArgument",
                format!("{} is an object, so it cannot also be a folder", &key[..i]),
            ));
        }
    }
    let dir = format!("{key}/");
    if files
        .range(dir.clone()..)
        .next()
        .is_some_and(|(k, _)| k.starts_with(&dir))
    {
        return Err(err(
            "InvalidArgument",
            format!("{key} is a folder of other objects, so it cannot also be an object"),
        ));
    }
    Ok(())
}

async fn commit_batch(
    st: &St,
    mut cred: Cred,
    bucket: &str,
    batch: &[Waiter],
) -> Vec<Result<u32, S3Error>> {
    let all = |e: S3Error| vec![Err(e); batch.len()];
    let cache = match st.s3.cache().await {
        Ok(c) => c,
        Err(e) => return all(e),
    };
    for attempt in 0..MAX_COMMIT_TRIES {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
        }
        let l = match listing(st, &mut cred, bucket, true).await {
            Ok(Some(l)) => l,
            Ok(None) => return all(err("NoSuchBucket", format!("no bucket {bucket}"))),
            Err(e) => return all(e),
        };
        let before: BTreeMap<String, blobs::Hash> = l
            .objects
            .iter()
            .map(|o| (o.key.clone(), blobs::to_hash(&o.hash)))
            .collect();
        let mut files = before.clone();
        let mut meta: BTreeMap<String, Option<S3Meta>> = BTreeMap::new();
        let mut results: Vec<Result<(), S3Error>> = Vec::with_capacity(batch.len());
        for w in batch {
            let mut trial = files.clone();
            let mut r = Ok(());
            for (k, ch) in &w.changes {
                match ch {
                    Some((h, _)) => {
                        if let Err(e) = folder_clash(&trial, k) {
                            r = Err(e);
                            break;
                        }
                        trial.insert(k.clone(), *h);
                    }
                    None => {
                        trial.remove(k);
                    }
                }
            }
            if r.is_ok() {
                files = trial;
                for (k, ch) in &w.changes {
                    meta.insert(k.clone(), ch.as_ref().map(|(_, m)| m.clone()));
                }
            }
            results.push(r);
        }
        let changed = files != before || meta.values().any(Option::is_some);
        if !changed {
            return results.into_iter().map(|r| r.map(|()| l.version)).collect();
        }
        let mut c = blobs::Collection::default();
        for (k, h) in &files {
            c.push(k.clone(), *h);
        }
        let root = match c.store(&cache.store).await {
            Ok(t) => t,
            Err(e) => return all(internal(format!("store the collection: {e}"))),
        };
        let pushed = async {
            let client = st
                .backend
                .client(cred.token)
                .await
                .map_err(|e| err("ServiceUnavailable", format!("{e:#}")))?;
            client
                .push_collection(&cache.store, root.hash())
                .await
                .map_err(from_cp)
        }
        .await;
        if let Err(e) = pushed {
            return all(e);
        }
        let resp = call(
            st,
            &mut cred,
            AdminOp::S3Commit {
                name: bucket.to_string(),
                base: Some(l.version),
                root: blobs::from_hash(root.hash()),
                account: None,
                meta: meta.into_iter().collect(),
            },
        )
        .await;
        st.s3.forget_listings(bucket);
        match resp {
            Ok(AdminResponse::Datasets(v)) if !v.is_empty() => {
                let version = v[0].version;
                return results.into_iter().map(|r| r.map(|()| version)).collect();
            }
            Ok(_) => return all(internal("unexpected response")),
            Err(e) if e.code == "OperationAborted" => continue,
            Err(e) => return all(e),
        }
    }
    all(err(
        "OperationAborted",
        "the bucket kept changing during this write; try again",
    ))
}

async fn create_bucket(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    body: Body,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    pumasi_proto::store::validate_dataset_name(bucket)
        .map_err(|e| err("InvalidBucketName", format!("{e:#}")))?;
    // The body may name a region; any region will do.
    Payload::new(body, headers, cred, MAX_XML)?
        .read_all()
        .await?;
    if listing(st, cred, bucket, true).await?.is_some() {
        return Err(err(
            "BucketAlreadyOwnedByYou",
            format!("bucket {bucket} already exists"),
        ));
    }
    let cache = st.s3.cache().await?;
    let root = blobs::Collection::default()
        .store(&cache.store)
        .await
        .map_err(|e| internal(format!("store the collection: {e}")))?;
    let client = st
        .backend
        .client(cred.token)
        .await
        .map_err(|e| err("ServiceUnavailable", format!("{e:#}")))?;
    client
        .push_collection(&cache.store, root.hash())
        .await
        .map_err(from_cp)?;
    call(
        st,
        cred,
        AdminOp::S3Commit {
            name: bucket.to_string(),
            base: None,
            root: blobs::from_hash(root.hash()),
            account: None,
            meta: Vec::new(),
        },
    )
    .await
    .map_err(|e| {
        if e.code == "OperationAborted" {
            err(
                "BucketAlreadyOwnedByYou",
                format!("bucket {bucket} already exists"),
            )
        } else {
            e
        }
    })?;
    st.s3.forget_listings(bucket);
    let mut resp = StatusCode::OK.into_response();
    if let Some(v) = header_value(&format!("/{bucket}")) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    Ok(resp)
}

async fn delete_bucket(st: &St, cred: &mut Cred, bucket: &str) -> Result<Response, S3Error> {
    let l = listing(st, cred, bucket, true)
        .await?
        .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
    if !l.objects.is_empty() {
        return Err(err(
            "BucketNotEmpty",
            format!("bucket {bucket} still has {} objects", l.objects.len()),
        ));
    }
    call(
        st,
        cred,
        AdminOp::StoreRemove {
            name: bucket.to_string(),
            version: None,
        },
    )
    .await?;
    st.s3.forget_listings(bucket);
    Ok(StatusCode::NO_CONTENT.into_response())
}

fn etag_response(etag: &str) -> Response {
    let mut resp = StatusCode::OK.into_response();
    resp.headers_mut()
        .insert(header::ETAG, header_value(&format!("\"{etag}\"")).unwrap());
    resp
}

async fn put_object(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    key: &str,
    body: Body,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let meta = user_meta(headers)?;
    let content_type = hdr(headers, "content-type").map(str::to_string);
    // Folder markers (`dir/`): datasets have no empty folders.
    if key.ends_with('/') {
        let data = Payload::new(body, headers, cred, 0)?.read_all().await?;
        let _ = data;
        return Ok(etag_response("d41d8cd98f00b204e9800998ecf8427e"));
    }
    check_key(key)?;
    let cache = st.s3.cache().await?;
    let p = Payload::new(body, headers, cred, st.upload_limit)?;
    let (tag, _size, md5) = store_body(cache, p).await?;
    let etag = data_encoding::HEXLOWER.encode(&md5);
    let hash = tag.hash();
    commit(
        st,
        cred,
        bucket,
        vec![(
            key.to_string(),
            Some((
                hash,
                S3Meta {
                    hash: blobs::from_hash(hash),
                    etag: etag.clone(),
                    content_type,
                    meta,
                    modified_unix: now_unix(),
                },
            )),
        )],
    )
    .await?;
    drop(tag);
    Ok(etag_response(&etag))
}

async fn delete_object(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    key: &str,
) -> Result<Response, S3Error> {
    let l = listing(st, cred, bucket, true)
        .await?
        .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
    if l.get(key).is_some() {
        commit(st, cred, bucket, vec![(key.to_string(), None)]).await?;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn delete_objects(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    body: Body,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let body = Payload::new(body, headers, cred, MAX_XML)?
        .read_all()
        .await?;
    let body = String::from_utf8(body).map_err(|_| err("MalformedXML", "the body is not text"))?;
    let quiet = xml::text(&body, "Quiet").as_deref() == Some("true");
    let keys: Vec<String> = xml::all(&body, "Object")
        .into_iter()
        .filter_map(|o| xml::text(o, "Key"))
        .collect();
    if keys.len() > MAX_KEYS {
        return Err(err("MalformedXML", "at most 1000 keys at a time"));
    }
    let l = listing(st, cred, bucket, true)
        .await?
        .ok_or_else(|| err("NoSuchBucket", format!("no bucket {bucket}")))?;
    let gone: Vec<Change> = keys
        .iter()
        .filter(|k| l.get(k).is_some())
        .map(|k| (k.clone(), None))
        .collect();
    let failed = if gone.is_empty() {
        None
    } else {
        commit(st, cred, bucket, gone).await.err()
    };
    let mut x = format!("<DeleteResult xmlns=\"{}\">", xml::NS);
    for k in &keys {
        match &failed {
            Some(e) => x.push_str(&format!(
                "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
                esc(k),
                e.code,
                esc(&e.msg)
            )),
            None if !quiet => x.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", esc(k))),
            None => {}
        }
    }
    x.push_str("</DeleteResult>");
    Ok(xml_response(StatusCode::OK, x))
}

async fn copy_object(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    check_key(key)?;
    let source = hdr(headers, "x-amz-copy-source").unwrap_or("");
    let source = source.split('?').next().unwrap_or("");
    let source = sigv4::decode(source.trim_start_matches('/'));
    let (sb, sk) = source
        .split_once('/')
        .ok_or_else(|| err("InvalidArgument", "x-amz-copy-source must be bucket/key"))?;
    let l = listing(st, cred, sb, false)
        .await?
        .ok_or_else(|| err("NoSuchBucket", format!("no bucket {sb}")))?;
    let o = l
        .get(sk)
        .ok_or_else(|| err("NoSuchKey", format!("no object {sk} in {sb}")))?;
    let replace =
        hdr(headers, "x-amz-metadata-directive").is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"));
    let (content_type, meta) = if replace {
        (
            hdr(headers, "content-type").map(str::to_string),
            user_meta(headers)?,
        )
    } else {
        o.meta
            .as_ref()
            .map(|m| (m.content_type.clone(), m.meta.clone()))
            .unwrap_or_default()
    };
    let etag = o.etag();
    let now = now_unix();
    let hash = blobs::to_hash(&o.hash);
    commit(
        st,
        cred,
        bucket,
        vec![(
            key.to_string(),
            Some((
                hash,
                S3Meta {
                    hash: o.hash,
                    etag: etag.clone(),
                    content_type,
                    meta,
                    modified_unix: now,
                },
            )),
        )],
    )
    .await?;
    Ok(xml_response(
        StatusCode::OK,
        format!(
            "<CopyObjectResult xmlns=\"{}\"><ETag>&quot;{}&quot;</ETag><LastModified>{}</LastModified></CopyObjectResult>",
            xml::NS,
            esc(&etag),
            iso_date(now)
        ),
    ))
}

// -- multipart uploads ---------------------------------------------------------

fn create_upload(
    st: &St,
    cred: &Cred,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    check_key(key)?;
    let meta = user_meta(headers)?;
    let id = data_encoding::HEXLOWER.encode(&rand::random::<[u8; 16]>());
    {
        let mut ups = st.s3.uploads.lock().unwrap();
        ups.retain(|_, u| u.started.elapsed() < UPLOAD_EXPIRY);
        if ups.len() >= MAX_UPLOADS {
            return Err(err(
                "SlowDown",
                "too many multipart uploads are open; finish or abort some",
            ));
        }
        ups.insert(
            id.clone(),
            Upload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                owner: cred.key.clone(),
                started: Instant::now(),
                started_unix: now_unix(),
                content_type: hdr(headers, "content-type").map(str::to_string),
                meta,
                parts: BTreeMap::new(),
            },
        );
    }
    Ok(xml_response(
        StatusCode::OK,
        format!(
            "<InitiateMultipartUploadResult xmlns=\"{}\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>",
            xml::NS,
            esc(bucket),
            esc(key)
        ),
    ))
}

fn no_upload(id: &str) -> S3Error {
    err("NoSuchUpload", format!("no multipart upload {id}"))
}

/// Checks that an upload exists and belongs to the caller.
fn check_upload(st: &St, cred: &Cred, bucket: &str, key: &str, id: &str) -> Result<(), S3Error> {
    let ups = st.s3.uploads.lock().unwrap();
    match ups.get(id) {
        Some(u) if u.bucket == bucket && u.key == key && u.owner == cred.key => Ok(()),
        _ => Err(no_upload(id)),
    }
}

async fn upload_part(
    st: &St,
    cred: &Cred,
    bucket: &str,
    key: &str,
    q: &[(String, String)],
    body: Body,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let qv = |k: &str| q.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    let id = qv("uploadId").unwrap_or("");
    let number: u32 = qv("partNumber")
        .and_then(|n| n.parse().ok())
        .filter(|n| (1..=MAX_PARTS).contains(n))
        .ok_or_else(|| err("InvalidArgument", "partNumber must be 1 to 10000"))?;
    check_upload(st, cred, bucket, key, id)?;
    let cache = st.s3.cache().await?;
    let p = Payload::new(body, headers, cred, st.upload_limit)?;
    let (tag, size, md5) = store_body(cache, p).await?;
    let hash = tag.hash();
    {
        let mut ups = st.s3.uploads.lock().unwrap();
        let u = ups.get_mut(id).ok_or_else(|| no_upload(id))?;
        u.parts.insert(
            number,
            Part {
                _tag: tag,
                hash,
                size,
                md5,
                at_unix: now_unix(),
            },
        );
    }
    Ok(etag_response(&data_encoding::HEXLOWER.encode(&md5)))
}

fn abort_upload(
    st: &St,
    cred: &Cred,
    bucket: &str,
    key: &str,
    id: &str,
) -> Result<Response, S3Error> {
    check_upload(st, cred, bucket, key, id)?;
    st.s3.uploads.lock().unwrap().remove(id);
    Ok(StatusCode::NO_CONTENT.into_response())
}

fn list_parts(
    st: &St,
    cred: &Cred,
    bucket: &str,
    key: &str,
    id: &str,
) -> Result<Response, S3Error> {
    check_upload(st, cred, bucket, key, id)?;
    let ups = st.s3.uploads.lock().unwrap();
    let u = ups.get(id).ok_or_else(|| no_upload(id))?;
    let mut x = format!(
        "<ListPartsResult xmlns=\"{}\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{id}</UploadId><PartNumberMarker>0</PartNumberMarker><MaxParts>{MAX_PARTS}</MaxParts><IsTruncated>false</IsTruncated>",
        xml::NS,
        esc(bucket),
        esc(key)
    );
    for (n, p) in &u.parts {
        x.push_str(&format!(
            "<Part><PartNumber>{n}</PartNumber><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size></Part>",
            iso_date(p.at_unix),
            data_encoding::HEXLOWER.encode(&p.md5),
            p.size
        ));
    }
    x.push_str("</ListPartsResult>");
    Ok(xml_response(StatusCode::OK, x))
}

fn list_uploads(st: &St, cred: &Cred, bucket: &str) -> Result<Response, S3Error> {
    let ups = st.s3.uploads.lock().unwrap();
    let mut x = format!(
        "<ListMultipartUploadsResult xmlns=\"{}\"><Bucket>{}</Bucket><KeyMarker/><UploadIdMarker/><MaxUploads>{MAX_UPLOADS}</MaxUploads><IsTruncated>false</IsTruncated>",
        xml::NS,
        esc(bucket)
    );
    let mut mine: Vec<(&String, &Upload)> = ups
        .iter()
        .filter(|(_, u)| u.bucket == bucket && u.owner == cred.key)
        .collect();
    mine.sort_by(|a, b| (&a.1.key, a.0).cmp(&(&b.1.key, b.0)));
    for (id, u) in mine {
        x.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{id}</UploadId><Initiated>{}</Initiated><StorageClass>STANDARD</StorageClass></Upload>",
            esc(&u.key),
            iso_date(u.started_unix)
        ));
    }
    x.push_str("</ListMultipartUploadsResult>");
    Ok(xml_response(StatusCode::OK, x))
}

#[allow(clippy::too_many_arguments)]
async fn complete_upload(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    key: &str,
    id: &str,
    body: Body,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    check_upload(st, cred, bucket, key, id)?;
    let body = Payload::new(body, headers, cred, MAX_XML)?
        .read_all()
        .await?;
    let body = String::from_utf8(body).map_err(|_| err("MalformedXML", "the body is not text"))?;
    let mut asked: Vec<(u32, String)> = Vec::new();
    for p in xml::all(&body, "Part") {
        let n = xml::text(p, "PartNumber")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| err("MalformedXML", "a part without a PartNumber"))?;
        let etag = xml::text(p, "ETag").unwrap_or_default();
        asked.push((n, etag.trim_matches('"').to_string()));
    }
    if asked.is_empty() {
        return Err(err("MalformedXML", "no parts to complete the upload with"));
    }
    if asked.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(err(
            "InvalidPartOrder",
            "parts must be listed in ascending order",
        ));
    }
    let Some(mut upload) = st.s3.uploads.lock().unwrap().remove(id) else {
        return Err(no_upload(id));
    };
    let result = finish_upload(st, cred, bucket, key, &upload, &asked).await;
    if result.is_err() {
        // Let the client try again (or abort).
        upload.started = Instant::now();
        st.s3.uploads.lock().unwrap().insert(id.to_string(), upload);
    }
    let etag = result?;
    Ok(xml_response(
        StatusCode::OK,
        format!(
            "<CompleteMultipartUploadResult xmlns=\"{}\"><Location>/s3/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>&quot;{etag}&quot;</ETag></CompleteMultipartUploadResult>",
            xml::NS,
            esc(bucket),
            esc(&sigv4::encode(key, true)),
            esc(bucket),
            esc(key)
        ),
    ))
}

/// Joins an upload's parts into one blob and commits it; returns its ETag.
async fn finish_upload(
    st: &St,
    cred: &mut Cred,
    bucket: &str,
    key: &str,
    upload: &Upload,
    asked: &[(u32, String)],
) -> Result<String, S3Error> {
    let mut parts = Vec::with_capacity(asked.len());
    let mut md5s = Md5::new();
    let mut total = 0u64;
    for (n, etag) in asked {
        let p = upload
            .parts
            .get(n)
            .ok_or_else(|| err("InvalidPart", format!("part {n} was not uploaded")))?;
        if !etag.is_empty() && *etag != data_encoding::HEXLOWER.encode(&p.md5) {
            return Err(err(
                "InvalidPart",
                format!("part {n}'s ETag does not match"),
            ));
        }
        md5s.update(p.md5);
        total += p.size;
        parts.push((p.hash, p.size));
    }
    if total > st.upload_limit {
        return Err(err(
            "EntityTooLarge",
            format!(
                "the object is larger than this gateway's upload limit ({} bytes)",
                st.upload_limit
            ),
        ));
    }
    let etag = format!(
        "{}-{}",
        data_encoding::HEXLOWER.encode(&md5s.finalize()),
        asked.len()
    );
    let cache = st.s3.cache().await?;
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(8);
    let store = cache.store.clone();
    let reader = AbortOnDrop(tokio::spawn(async move {
        for (hash, size) in parts {
            let mut s = std::pin::pin!(store.blobs().export_ranges(hash, 0..size).stream());
            while let Some(item) = s.next().await {
                if let Some(b) = clip(item, 0, size)
                    && tx.send(b).await.is_err()
                {
                    return;
                }
            }
        }
    }));
    let stream =
        n0_future::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|x| (x, rx)) });
    let tag = cache
        .store
        .blobs()
        .add_stream(stream)
        .await
        .temp_tag()
        .await
        .map_err(|e| internal(format!("join the parts: {e}")))?;
    drop(reader);
    let hash = tag.hash();
    let size = blobs::blob_size(&cache.store, hash)
        .await
        .map_err(internal)?;
    if size != total {
        return Err(internal("the joined parts have the wrong size"));
    }
    commit(
        st,
        cred,
        bucket,
        vec![(
            key.to_string(),
            Some((
                hash,
                S3Meta {
                    hash: blobs::from_hash(hash),
                    etag: etag.clone(),
                    content_type: upload.content_type.clone(),
                    meta: upload.meta.clone(),
                    modified_unix: now_unix(),
                },
            )),
        )],
    )
    .await?;
    drop(tag);
    Ok(etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(parse_range(Some("bytes=0-9"), 100).unwrap(), Some((0, 10)));
        assert_eq!(
            parse_range(Some("bytes=90-"), 100).unwrap(),
            Some((90, 100))
        );
        assert_eq!(parse_range(Some("bytes=-5"), 100).unwrap(), Some((95, 100)));
        assert_eq!(
            parse_range(Some("bytes=50-500"), 100).unwrap(),
            Some((50, 100))
        );
        assert!(parse_range(Some("bytes=100-"), 100).is_err());
        assert_eq!(parse_range(Some("bytes=0-1,5-6"), 100).unwrap(), None);
        assert_eq!(parse_range(None, 100).unwrap(), None);
    }

    #[test]
    fn folders_and_files_do_not_clash_silently() {
        let mut f = BTreeMap::new();
        f.insert("a/b".to_string(), blobs::Hash::new(b"x"));
        f.insert("c".to_string(), blobs::Hash::new(b"y"));
        assert!(folder_clash(&f, "a").is_err());
        assert!(folder_clash(&f, "c/d").is_err());
        assert!(folder_clash(&f, "a/c").is_ok());
        assert!(folder_clash(&f, "ab").is_ok());
    }
}
