//! AWS Signature Version 4 as S3 uses it (1.8), shared by the gateway in
//! `pumasi ui`, which builds the string to sign from the request, and the
//! control plane, which holds the keys' secrets and checks signatures.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// The prefix of access key ids.
pub const KEY_PREFIX: &str = "PUMA";
/// The service name in a signature's scope.
pub const SERVICE: &str = "s3";
/// How far a request's `x-amz-date` may be from the control plane's clock.
pub const MAX_SKEW_SECS: i64 = 15 * 60;

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC takes any key length");
    m.update(data);
    m.finalize().into_bytes().into()
}

/// The day's signing key: `HMAC(HMAC(HMAC(HMAC("AWS4" + secret, date),
/// region), "s3"), "aws4_request")`.
pub fn signing_key(secret: &str, date: &str, region: &str) -> [u8; 32] {
    let k = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, SERVICE.as_bytes());
    hmac(&k, b"aws4_request")
}

/// The hex signature of `data` with a signing key.
pub fn sign(key: &[u8; 32], data: &str) -> String {
    data_encoding::HEXLOWER.encode(&hmac(key, data.as_bytes()))
}

/// Lowercase hex SHA-256.
pub fn sha256_hex(data: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(data))
}

/// Compares two strings in time that depends only on their lengths.
pub fn same(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// Checks the parts of a string to sign that the control plane can: the
/// algorithm, that the scope is `date/region/s3/aws4_request`, and that
/// its time is within [`MAX_SKEW_SECS`] of `now_unix`.
pub fn check_string_to_sign(sts: &str, date: &str, region: &str, now_unix: u64) -> Result<(), String> {
    let mut lines = sts.split('\n');
    let algo = lines.next().unwrap_or("");
    if algo != "AWS4-HMAC-SHA256" {
        return Err(format!("unsupported signature algorithm {algo:?}"));
    }
    let time = lines.next().unwrap_or("");
    let scope = lines.next().unwrap_or("");
    if lines.next().is_none_or(|h| h.len() != 64) || lines.next().is_some() {
        return Err("malformed string to sign".into());
    }
    if scope != format!("{date}/{region}/{SERVICE}/aws4_request") {
        return Err("the signature's scope does not match".into());
    }
    if !time.starts_with(date) {
        return Err("the request time is not on the scope's day".into());
    }
    let t = parse_amz_date(time).ok_or("malformed x-amz-date")?;
    if (t - now_unix as i64).abs() > MAX_SKEW_SECS {
        return Err("RequestTimeTooSkewed".into());
    }
    Ok(())
}

/// `20130524T000000Z` as Unix seconds.
pub fn parse_amz_date(s: &str) -> Option<i64> {
    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%SZ").ok()?;
    Some(dt.and_utc().timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example of the S3 documentation ("GET Object" in "Signature
    /// Calculations for the Authorization Header").
    #[test]
    fn aws_documentation_example() {
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let canonical = "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\n\
            x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
            x-amz-date:20130524T000000Z\n\nhost;range;x-amz-content-sha256;x-amz-date\n\
            e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let sts = format!(
            "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
            sha256_hex(canonical.as_bytes())
        );
        let key = signing_key(secret, "20130524", "us-east-1");
        assert_eq!(
            sign(&key, &sts),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        let now = parse_amz_date("20130524T000500Z").unwrap() as u64;
        assert!(check_string_to_sign(&sts, "20130524", "us-east-1", now).is_ok());
        assert!(check_string_to_sign(&sts, "20130524", "eu-west-1", now).is_err());
        assert!(check_string_to_sign(&sts, "20130524", "us-east-1", now + 3600).is_err());
    }
}
