//! The audit log (phase 17): who changed what in the cluster, as a hash
//! chain.
//!
//! Every entry carries the BLAKE3 hash of the one before it (`prev`) and
//! its own hash over all its fields, so changing, removing or reordering
//! an entry breaks every hash after it. The control plane signs the newest
//! hash (the head) with the cluster key on request ([`AuditHead`]); a head
//! kept somewhere else, or the copy a syslog server received, shows later
//! whether the stored log was rewritten.
//!
//! The chain proves the log was not edited after the fact; it cannot stop
//! someone holding the cluster key and the state database from writing a
//! whole new chain. That is what exporting to another machine is for.

use std::fmt::Write as _;

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// The `prev` of the first entry.
pub const GENESIS: [u8; 32] = [0; 32];

/// One audit entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// 1, 2, 3, ... with no gaps.
    pub seq: u64,
    pub unix: u64,
    /// Who: `cluster admin`, an access ticket's name, `sso:<email>`,
    /// `ldap:<user>`, `node:<name>`, or `control plane`.
    pub actor: String,
    /// The actor's role: `admin`, `submitter (<account>)`, `viewer`,
    /// `node`, `system` or `anonymous`.
    pub role: String,
    /// The iroh endpoint the call came from (short form), if any.
    pub peer: String,
    /// What, as `area.verb`: `job.submit`, `access.create`, ...
    pub action: String,
    /// What it was done to: a job id, node, account, ticket name.
    pub target: String,
    /// More about it (never a secret's value).
    pub detail: String,
    /// Whether it worked. Refused and failed calls are logged too.
    pub ok: bool,
    #[serde(with = "hex32")]
    pub prev: [u8; 32],
    #[serde(with = "hex32")]
    pub hash: [u8; 32],
}

/// The fields an entry's hash covers, in order.
#[derive(Serialize)]
struct Body<'a> {
    seq: u64,
    unix: u64,
    actor: &'a str,
    role: &'a str,
    peer: &'a str,
    action: &'a str,
    target: &'a str,
    detail: &'a str,
    ok: bool,
    prev: &'a [u8; 32],
}

impl AuditEntry {
    /// The hash this entry should carry.
    pub fn compute_hash(&self) -> [u8; 32] {
        let body = Body {
            seq: self.seq,
            unix: self.unix,
            actor: &self.actor,
            role: &self.role,
            peer: &self.peer,
            action: &self.action,
            target: &self.target,
            detail: &self.detail,
            ok: self.ok,
            prev: &self.prev,
        };
        let mut h = blake3::Hasher::new();
        h.update(b"pumasi/audit/v1\0");
        h.update(&postcard::to_stdvec(&body).expect("audit entry encodes"));
        *h.finalize().as_bytes()
    }

    /// Fills in `prev` and `hash` after `prev`.
    pub fn seal(mut self, prev: [u8; 32]) -> Self {
        self.prev = prev;
        self.hash = self.compute_hash();
        self
    }

    /// One line for people: `#12 2026-11-06T06:38:27Z alice (admin) job.submit 3f2a.. ok`.
    pub fn line(&self) -> String {
        let mut s = format!(
            "#{} {} {} ({}) {}",
            self.seq,
            rfc3339(self.unix),
            self.actor,
            self.role,
            self.action
        );
        if !self.target.is_empty() {
            let _ = write!(s, " {}", self.target);
        }
        s.push_str(if self.ok { " ok" } else { " FAILED" });
        if !self.detail.is_empty() {
            let _ = write!(s, ": {}", self.detail);
        }
        s
    }
}

/// Checks that `entries` follow each other and `prev` (the hash before
/// the first one, [`GENESIS`] for the start of the log). Returns the last
/// hash.
pub fn verify_chain(prev: [u8; 32], first_seq: u64, entries: &[AuditEntry]) -> Result<[u8; 32]> {
    let mut prev = prev;
    let mut seq = first_seq;
    for e in entries {
        ensure!(
            e.seq == seq,
            "audit entry #{} where #{seq} should be (an entry is missing or out of order)",
            e.seq
        );
        ensure!(
            e.prev == prev,
            "audit entry #{} does not follow the entry before it (the log was changed)",
            e.seq
        );
        ensure!(
            e.compute_hash() == e.hash,
            "audit entry #{} does not match its hash (it was changed)",
            e.seq
        );
        prev = e.hash;
        seq += 1;
    }
    Ok(prev)
}

/// The newest entry's hash, signed by the cluster key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditHead {
    /// The cluster key (its iroh endpoint id), which signed.
    pub cluster: String,
    /// Entries checked, from #1.
    pub entries: u64,
    /// The newest entry (0 for an empty log).
    pub seq: u64,
    #[serde(with = "hex32")]
    pub hash: [u8; 32],
    pub unix: u64,
    /// Ed25519 over [`AuditHead::signed_bytes`].
    #[serde(with = "hex_vec")]
    pub signature: Vec<u8>,
}

impl AuditHead {
    pub fn signed_bytes(seq: u64, hash: &[u8; 32], unix: u64) -> Vec<u8> {
        let mut b = b"pumasi/audit-head/v1\0".to_vec();
        b.extend_from_slice(&seq.to_le_bytes());
        b.extend_from_slice(hash);
        b.extend_from_slice(&unix.to_le_bytes());
        b
    }

    /// Checks the signature against the cluster key it names.
    pub fn verify(&self) -> Result<()> {
        let key: iroh::PublicKey = self
            .cluster
            .parse()
            .map_err(|e| anyhow::anyhow!("bad cluster key in audit head: {e}"))?;
        let Ok(sig) = <[u8; 64]>::try_from(self.signature.as_slice()) else {
            bail!("audit head signature has the wrong length");
        };
        key.verify(
            &Self::signed_bytes(self.seq, &self.hash, self.unix),
            &iroh::Signature::from_bytes(&sig),
        )
        .map_err(|_| anyhow::anyhow!("audit head signature does not verify"))
    }
}

/// UTC time as RFC 3339 (`2026-11-06T06:38:27Z`).
pub fn rfc3339(unix: u64) -> String {
    let days = (unix / 86_400) as i64;
    let secs = unix % 86_400;
    // Civil from days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

/// An entry as an RFC 5424 syslog message (facility `authpriv`, severity
/// notice, or warning for a failure), with the whole entry as JSON in the
/// message so a collector keeps the hashes. `host` and `app` go into the
/// header.
pub fn syslog_line(e: &AuditEntry, host: &str) -> String {
    // authpriv = 10; notice = 5, warning = 4.
    let pri = 10 * 8 + if e.ok { 5 } else { 4 };
    let host = header_token(host);
    let json = serde_json::to_string(e).expect("audit entry serializes");
    format!(
        "<{pri}>1 {} {host} pumasi - audit [pumasi@32473 seq=\"{}\" action=\"{}\"] {json}",
        rfc3339(e.unix),
        e.seq,
        sd_escape(&e.action),
    )
}

/// A header field: printable ASCII without spaces, or `-`.
fn header_token(s: &str) -> String {
    let t: String = s
        .chars()
        .filter(|c| c.is_ascii_graphic())
        .take(255)
        .collect();
    if t.is_empty() { "-".into() } else { t }
}

/// Escapes `"`, `\` and `]` in a structured-data value.
fn sd_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '"' | '\\' | ']') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// 32 bytes as hex in human-readable formats (JSON), bytes otherwise.
pub mod hex32 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&data_encoding::HEXLOWER.encode(v))
        } else {
            serde::Serialize::serialize(v, s)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            let v = data_encoding::HEXLOWER_PERMISSIVE
                .decode(s.as_bytes())
                .map_err(serde::de::Error::custom)?;
            v.try_into()
                .map_err(|_| serde::de::Error::custom("expected 32 bytes of hex"))
        } else {
            <[u8; 32]>::deserialize(d)
        }
    }
}

/// Bytes as hex in human-readable formats.
pub mod hex_vec {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&data_encoding::HEXLOWER.encode(v))
        } else {
            s.serialize_bytes(v)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            data_encoding::HEXLOWER_PERMISSIVE
                .decode(s.as_bytes())
                .map_err(serde::de::Error::custom)
        } else {
            <Vec<u8>>::deserialize(d)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seq: u64, action: &str) -> AuditEntry {
        AuditEntry {
            seq,
            unix: 1_793_000_000 + seq,
            actor: "alice".into(),
            role: "admin".into(),
            peer: "abc123".into(),
            action: action.into(),
            target: "job 1".into(),
            detail: String::new(),
            ok: true,
            prev: GENESIS,
            hash: GENESIS,
        }
    }

    fn chain(n: u64) -> Vec<AuditEntry> {
        let mut prev = GENESIS;
        (1..=n)
            .map(|i| {
                let e = entry(i, "job.submit").seal(prev);
                prev = e.hash;
                e
            })
            .collect()
    }

    #[test]
    fn a_chain_verifies_and_any_change_breaks_it() {
        let v = chain(5);
        let head = verify_chain(GENESIS, 1, &v).unwrap();
        assert_eq!(head, v[4].hash);
        // From the middle, given the hash before.
        assert_eq!(verify_chain(v[1].hash, 3, &v[2..]).unwrap(), head);

        let mut edited = v.clone();
        edited[2].actor = "mallory".into();
        assert!(verify_chain(GENESIS, 1, &edited).is_err());

        let mut rehashed = v.clone();
        rehashed[2].ok = false;
        rehashed[2].hash = rehashed[2].compute_hash();
        let err = verify_chain(GENESIS, 1, &rehashed).unwrap_err();
        assert!(err.to_string().contains("#4"), "{err}");

        let mut dropped = v.clone();
        dropped.remove(1);
        assert!(verify_chain(GENESIS, 1, &dropped).is_err());

        let mut swapped = v;
        swapped.swap(1, 2);
        assert!(verify_chain(GENESIS, 1, &swapped).is_err());
    }

    #[test]
    fn json_has_hex_hashes_and_round_trips() {
        let e = chain(1).remove(0);
        let j = serde_json::to_value(&e).unwrap();
        assert_eq!(j["hash"].as_str().unwrap().len(), 64);
        let back: AuditEntry = serde_json::from_value(j).unwrap();
        assert_eq!(back, e);
        let bin = postcard::to_stdvec(&e).unwrap();
        assert_eq!(postcard::from_bytes::<AuditEntry>(&bin).unwrap(), e);
    }

    #[test]
    fn signed_heads() {
        let key = iroh::SecretKey::from_bytes(&[9; 32]);
        let hash = [5; 32];
        let sig = key.sign(&AuditHead::signed_bytes(3, &hash, 100));
        let mut head = AuditHead {
            cluster: key.public().to_string(),
            entries: 3,
            seq: 3,
            hash,
            unix: 100,
            signature: sig.to_bytes().to_vec(),
        };
        head.verify().unwrap();
        let j = serde_json::to_string(&head).unwrap();
        serde_json::from_str::<AuditHead>(&j).unwrap().verify().unwrap();
        head.seq = 2;
        assert!(head.verify().is_err());
    }

    #[test]
    fn times_and_syslog() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339(1_793_947_107), "2026-11-06T06:38:27Z");
        let mut e = chain(1).remove(0);
        e.action = "a\"b]".into();
        let l = syslog_line(&e, "my host");
        assert!(l.starts_with("<85>1 "), "{l}");
        assert!(l.contains(" myhost pumasi - audit [pumasi@32473 seq=\"1\" action=\"a\\\"b\\]\"] {"));
        e.ok = false;
        assert!(syslog_line(&e, "").starts_with("<84>1 "));
        assert!(syslog_line(&e, "").contains(" - pumasi - audit "));
    }
}
