//! Reading a request's AWS Signature Version 4: the `Authorization`
//! header or a presigned URL's query, and the canonical request and
//! string to sign it covers. The secret never comes here: the control
//! plane checks the signature ([`pumasi_proto::msg::AdminOp::S3Auth`]).

use axum::http::{HeaderMap, Method, Uri};
use pumasi_proto::s3::sha256_hex;

/// The SHA-256 of an empty body.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// What a request's signature claims.
#[derive(Debug, Clone)]
pub struct Signed {
    pub key: String,
    /// `YYYYMMDD`.
    pub date: String,
    pub region: String,
    /// `x-amz-date` (`YYYYMMDDTHHMMSSZ`).
    pub amz_date: String,
    pub signature: String,
    pub string_to_sign: String,
    /// How the body is signed: a hex SHA-256, `UNSIGNED-PAYLOAD`, or a
    /// `STREAMING-...` mode.
    pub payload: String,
    /// A presigned URL's lifetime.
    pub expires: Option<u32>,
}

impl Signed {
    pub fn scope(&self) -> String {
        format!("{}/{}/s3/aws4_request", self.date, self.region)
    }
}

/// Why a request's signature cannot be read.
#[derive(Debug)]
pub struct Bad(pub &'static str, pub String);

fn bad(code: &'static str, msg: impl Into<String>) -> Bad {
    Bad(code, msg.into())
}

/// Percent-encodes as SigV4 does: everything but `A-Za-z0-9-._~` (and `/`
/// when `slash`).
pub fn encode(s: &str, slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Percent-decodes (`+` stays `+`).
pub fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            out.push(h * 16 + l);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// The query's parameters, decoded, in order.
pub fn query_pairs(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), String::new()),
        })
        .collect()
}

fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

/// Reads the signature of a request: its `Authorization` header or a
/// presigned query. `Ok(None)` for an anonymous request.
pub fn read(method: &Method, uri: &Uri, headers: &HeaderMap) -> Result<Option<Signed>, Bad> {
    let query = query_pairs(uri);
    let q = |k: &str| query.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    if header(headers, "authorization").is_some_and(|a| a.starts_with("AWS "))
        || q("AWSAccessKeyId").is_some()
    {
        return Err(bad(
            "InvalidRequest",
            "Signature Version 2 is not supported: sign with Signature Version 4 (s3v4)",
        ));
    }
    let (credential, signed_headers, signature, amz_date, payload, expires, presigned) =
        if let Some(auth) = header(headers, "authorization") {
            let rest = auth.strip_prefix("AWS4-HMAC-SHA256").ok_or_else(|| {
                bad(
                    "InvalidRequest",
                    "only AWS4-HMAC-SHA256 signatures are supported",
                )
            })?;
            let mut cred = None;
            let mut sh = None;
            let mut sig = None;
            for part in rest.split(',') {
                let part = part.trim();
                if let Some(v) = part.strip_prefix("Credential=") {
                    cred = Some(v.to_string());
                } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
                    sh = Some(v.to_string());
                } else if let Some(v) = part.strip_prefix("Signature=") {
                    sig = Some(v.to_string());
                }
            }
            let amz = header(headers, "x-amz-date")
                .ok_or_else(|| bad("AccessDenied", "x-amz-date is missing"))?
                .to_string();
            let payload = header(headers, "x-amz-content-sha256")
                .unwrap_or("UNSIGNED-PAYLOAD")
                .to_string();
            (
                cred.ok_or_else(|| bad("AuthorizationHeaderMalformed", "no Credential"))?,
                sh.ok_or_else(|| bad("AuthorizationHeaderMalformed", "no SignedHeaders"))?,
                sig.ok_or_else(|| bad("AuthorizationHeaderMalformed", "no Signature"))?,
                amz,
                payload,
                None,
                false,
            )
        } else if q("X-Amz-Algorithm").is_some() {
            if q("X-Amz-Algorithm") != Some("AWS4-HMAC-SHA256") {
                return Err(bad(
                    "InvalidRequest",
                    "only AWS4-HMAC-SHA256 signatures are supported",
                ));
            }
            let expires: u32 = q("X-Amz-Expires")
                .and_then(|e| e.parse().ok())
                .ok_or_else(|| bad("AuthorizationQueryParametersError", "X-Amz-Expires"))?;
            if expires > 7 * 86_400 {
                return Err(bad(
                    "AuthorizationQueryParametersError",
                    "X-Amz-Expires must be at most 604800 seconds",
                ));
            }
            (
                q("X-Amz-Credential")
                    .ok_or_else(|| bad("AuthorizationQueryParametersError", "X-Amz-Credential"))?
                    .to_string(),
                q("X-Amz-SignedHeaders")
                    .ok_or_else(|| bad("AuthorizationQueryParametersError", "X-Amz-SignedHeaders"))?
                    .to_string(),
                q("X-Amz-Signature")
                    .ok_or_else(|| bad("AuthorizationQueryParametersError", "X-Amz-Signature"))?
                    .to_string(),
                q("X-Amz-Date")
                    .ok_or_else(|| bad("AuthorizationQueryParametersError", "X-Amz-Date"))?
                    .to_string(),
                header(headers, "x-amz-content-sha256")
                    .unwrap_or("UNSIGNED-PAYLOAD")
                    .to_string(),
                Some(expires),
                true,
            )
        } else {
            return Ok(None);
        };
    // Credential=KEY/DATE/REGION/s3/aws4_request
    let parts: Vec<&str> = credential.split('/').collect();
    let [key, date, region, service, term] = parts.as_slice() else {
        return Err(bad("AuthorizationHeaderMalformed", "malformed Credential"));
    };
    if *service != "s3" || *term != "aws4_request" {
        return Err(bad(
            "AuthorizationHeaderMalformed",
            "the Credential's scope is not s3",
        ));
    }
    let canonical = canonical_request(
        method,
        uri,
        headers,
        &signed_headers,
        &payload,
        presigned.then_some("X-Amz-Signature"),
    )?;
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date}/{region}/s3/aws4_request\n{}",
        sha256_hex(canonical.as_bytes())
    );
    Ok(Some(Signed {
        key: key.to_string(),
        date: date.to_string(),
        region: region.to_string(),
        amz_date,
        signature,
        string_to_sign,
        payload,
        expires,
    }))
}

/// The canonical request SigV4 signs. `skip` leaves a query parameter out
/// (a presigned URL's own signature).
pub fn canonical_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    signed_headers: &str,
    payload: &str,
    skip: Option<&str>,
) -> Result<String, Bad> {
    let path = encode(&decode(uri.path()), true);
    let mut q: Vec<(String, String)> = query_pairs(uri)
        .into_iter()
        .filter(|(k, _)| Some(k.as_str()) != skip)
        .map(|(k, v)| (encode(&k, false), encode(&v, false)))
        .collect();
    q.sort();
    let query = q
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let mut canon_headers = String::new();
    for name in signed_headers.split(';') {
        let values: Vec<String> = headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect();
        if values.is_empty() {
            return Err(bad(
                "AccessDenied",
                format!("signed header {name} is missing"),
            ));
        }
        canon_headers.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    Ok(format!(
        "{method}\n{path}\n{query}\n{canon_headers}\n{signed_headers}\n{payload}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    /// The "GET Object" example of the S3 SigV4 documentation, read back
    /// from its request.
    #[test]
    fn reads_the_documentation_example() {
        let mut h = HeaderMap::new();
        h.insert(
            "host",
            HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
        );
        h.insert("range", HeaderValue::from_static("bytes=0-9"));
        h.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static(EMPTY_SHA256),
        );
        h.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
        h.insert(
            "authorization",
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,\
                 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,\
                 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            ),
        );
        let uri: Uri = "/test.txt".parse().unwrap();
        let s = read(&Method::GET, &uri, &h).unwrap().unwrap();
        assert_eq!(s.key, "AKIAIOSFODNN7EXAMPLE");
        let key = pumasi_proto::s3::signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );
        assert_eq!(pumasi_proto::s3::sign(&key, &s.string_to_sign), s.signature);
    }

    /// The presigned URL example of the S3 documentation.
    #[test]
    fn reads_a_presigned_url() {
        let mut h = HeaderMap::new();
        h.insert(
            "host",
            HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
        );
        let uri: Uri = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
            .parse()
            .unwrap();
        let s = read(&Method::GET, &uri, &h).unwrap().unwrap();
        assert_eq!(s.expires, Some(86400));
        let key = pumasi_proto::s3::signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );
        assert_eq!(pumasi_proto::s3::sign(&key, &s.string_to_sign), s.signature);
    }

    #[test]
    fn encodes_like_aws() {
        assert_eq!(encode("a b/c~d!", true), "a%20b/c~d%21");
        assert_eq!(encode("a/b", false), "a%2Fb");
        assert_eq!(decode("a%20b%2Fc+d"), "a b/c+d");
        assert_eq!(decode("100%"), "100%");
    }
}
