//! Agent upgrades driven by the cluster.
//!
//! The owner picks one `pumasi` binary per platform and runs `pumasi
//! upgrade rollout`. The control plane stores the binaries and signs a
//! [`Release`] for each with its endpoint key, the same ed25519 key agents
//! already know from joining. Agents that opted in fetch the binary for
//! their platform, check the signature against the cluster they joined, the
//! size and the SHA-256, try the new binary, swap it in and restart. A
//! binary whose signature, platform or hash does not match is never run.

use std::fmt;

use anyhow::{Context, Result, bail, ensure};
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const RELEASE_DOMAIN: &[u8] = b"pumasi/release/v1\0";

/// Largest binary a rollout accepts.
pub const MAX_BINARY_BYTES: u64 = 256 * 1024 * 1024;

/// True if release `version` is older than `than`, comparing the numeric
/// `major.minor.patch` (a pre-release such as `1.2.0-rc1` comes before
/// `1.2.0`; build metadata after `+` and a leading `v` are ignored). A
/// version either side cannot parse is never older, so odd test and CI
/// version strings still roll out.
pub fn is_older(version: &str, than: &str) -> bool {
    fn parse(v: &str) -> Option<([u64; 3], bool)> {
        let v = v.trim().trim_start_matches('v');
        let v = v.split('+').next()?;
        let (core, pre) = match v.split_once('-') {
            Some((c, _)) => (c, true),
            None => (v, false),
        };
        let mut n = [0u64; 3];
        let mut parts = core.split('.');
        for slot in &mut n {
            match parts.next() {
                Some(p) => *slot = p.parse().ok()?,
                None => break,
            }
        }
        if parts.next().is_some() {
            return None;
        }
        Some((n, pre))
    }
    match (parse(version), parse(than)) {
        (Some((a, a_pre)), Some((b, b_pre))) => a < b || (a == b && a_pre && !b_pre),
        _ => false,
    }
}

/// An operating system and CPU architecture, named like Rust's
/// `std::env::consts::{OS, ARCH}` (`linux`, `macos`, `windows`; `x86_64`,
/// `aarch64`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Target {
    pub os: String,
    pub arch: String,
}

impl Target {
    pub fn new(os: &str, arch: &str) -> Self {
        Self {
            os: os.into(),
            arch: arch.into(),
        }
    }

    /// The platform this binary was built for.
    pub fn host() -> Self {
        Self::new(std::env::consts::OS, std::env::consts::ARCH)
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.os, self.arch)
    }
}

impl std::str::FromStr for Target {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let (os, arch) = s
            .split_once(['/', '-'])
            .context("a target looks like linux/x86_64")?;
        Ok(Self::new(os, arch))
    }
}

/// SHA-256 of some bytes.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Incremental SHA-256, for files read in chunks.
#[derive(Default, Clone)]
pub struct Sha256Hasher(Sha256);

impl Sha256Hasher {
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

/// SHA-256 of a file, read in 1 MiB chunks.
pub fn sha256_file(path: &std::path::Path) -> Result<[u8; 32]> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut h = Sha256Hasher::default();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finish())
}

/// Lower-case hex.
pub fn hex(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(bytes)
}

/// One binary of a rollout, as the owner describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseFile {
    pub target: Target,
    /// The version baked into the binary.
    pub version: String,
    /// The protocol version baked into the binary.
    pub protocol: u32,
    pub sha256: [u8; 32],
    pub size: u64,
}

/// What the control plane signs for one binary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Release {
    /// Control plane public key.
    pub cluster: [u8; 32],
    pub file: ReleaseFile,
    pub created_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedRelease {
    pub release: Release,
    pub sig: Signature,
}

fn release_message(r: &Release) -> Vec<u8> {
    let mut m = RELEASE_DOMAIN.to_vec();
    m.extend(postcard::to_stdvec(r).expect("release encodes"));
    m
}

impl SignedRelease {
    pub fn sign(release: Release, key: &SecretKey) -> Self {
        let sig = key.sign(&release_message(&release));
        Self { release, sig }
    }

    /// Checks that `cluster` signed this release.
    pub fn verify(&self, cluster: &[u8; 32]) -> Result<()> {
        ensure!(
            &self.release.cluster == cluster,
            "release is for another cluster"
        );
        let key = PublicKey::from_bytes(cluster).context("bad cluster key")?;
        key.verify(&release_message(&self.release), &self.sig)
            .map_err(|_| anyhow::anyhow!("release signature does not match the cluster key"))
    }

    pub fn file(&self) -> &ReleaseFile {
        &self.release.file
    }
}

/// Version and protocol read from a binary's [`crate::BUILD_INFO`] marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    pub version: String,
    pub protocol: u32,
}

/// Finds the build marker in a binary without running it.
pub fn read_build_info(bytes: &[u8]) -> Option<BuildInfo> {
    const PREFIX: &[u8] = b"pumasi-build-info:v=";
    let mut start = 0;
    while let Some(pos) = find(&bytes[start..], PREFIX) {
        let at = start + pos + PREFIX.len();
        start = at;
        // The pattern itself also appears in binaries (this function's own
        // copy), followed by other bytes: keep only well-formed markers.
        let tail = &bytes[at..bytes.len().min(at + 96)];
        let Some(end) = tail.iter().position(|&b| b == b';') else {
            continue;
        };
        let version = &tail[..end];
        let ok = !version.is_empty()
            && version
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || b"+-._".contains(b));
        if !ok {
            continue;
        }
        let rest = &tail[end + 1..];
        let Some(rest) = rest.strip_prefix(b"p=") else {
            continue;
        };
        let Some(pend) = rest.iter().position(|&b| b == b';') else {
            continue;
        };
        let Some(protocol) = std::str::from_utf8(&rest[..pend])
            .ok()
            .and_then(|s| s.parse().ok())
        else {
            continue;
        };
        return Some(BuildInfo {
            version: String::from_utf8_lossy(version).into_owned(),
            protocol,
        });
    }
    None
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Reads the platform of an executable from its header: ELF (Linux),
/// Mach-O (macOS) or PE (Windows), x86_64 or aarch64.
pub fn detect_target(bytes: &[u8]) -> Result<Target> {
    let u16le = |at: usize| -> Option<u16> {
        Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
    };
    let u32le = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
    };
    if bytes.starts_with(b"\x7fELF") {
        ensure!(
            bytes.get(4) == Some(&2),
            "32-bit ELF binaries are not supported"
        );
        ensure!(
            bytes.get(5) == Some(&1),
            "big-endian ELF binaries are not supported"
        );
        let arch = match u16le(18) {
            Some(0x3e) => "x86_64",
            Some(0xb7) => "aarch64",
            other => bail!("unsupported ELF machine {other:?}"),
        };
        return Ok(Target::new("linux", arch));
    }
    if bytes.starts_with(&[0xcf, 0xfa, 0xed, 0xfe]) {
        let arch = match u32le(4) {
            Some(0x0100_0007) => "x86_64",
            Some(0x0100_000c) => "aarch64",
            other => bail!("unsupported Mach-O CPU type {other:?}"),
        };
        return Ok(Target::new("macos", arch));
    }
    if bytes.starts_with(&[0xca, 0xfe, 0xba, 0xbe]) {
        bail!("universal (fat) Mach-O binaries are not supported; give one per architecture");
    }
    if bytes.starts_with(b"MZ") {
        let pe = u32le(0x3c).context("truncated PE header")? as usize;
        ensure!(
            bytes.get(pe..pe + 4) == Some(b"PE\0\0"),
            "not a PE executable"
        );
        let arch = match u16le(pe + 4) {
            Some(0x8664) => "x86_64",
            Some(0xaa64) => "aarch64",
            other => bail!("unsupported PE machine {other:?}"),
        };
        return Ok(Target::new("windows", arch));
    }
    bail!("not an executable (expected ELF, Mach-O or PE)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_versions() {
        assert!(is_older("1.2.0", "1.2.1"));
        assert!(is_older("1.1.9", "1.2.0"));
        assert!(is_older("v0.9.0", "1.0.0"));
        assert!(is_older("1.2.0-rc1", "1.2.0"));
        assert!(is_older("1.2.0", "1.2.1+ci.abc1234"));
        assert!(!is_older("1.2.1", "1.2.1"));
        assert!(!is_older("1.2.1+ci.new", "1.2.1+ci.old"));
        assert!(!is_older("1.3.0", "1.2.9"));
        assert!(!is_older("1.2.0", "1.2.0-rc1"));
        assert!(!is_older("9.9.9-test", "1.2.1"));
        assert!(!is_older("ci-new", "1.2.1"));
        assert!(!is_older("1.2.0", "unknown"));
    }

    fn release(cluster: &SecretKey) -> Release {
        Release {
            cluster: *cluster.public().as_bytes(),
            file: ReleaseFile {
                target: Target::new("linux", "x86_64"),
                version: "1.2.3".into(),
                protocol: 5,
                sha256: sha256(b"binary"),
                size: 6,
            },
            created_unix: 1_700_000_000,
        }
    }

    #[test]
    fn signature_binds_cluster_and_contents() {
        let key = SecretKey::from_bytes(&[7; 32]);
        let other = SecretKey::from_bytes(&[8; 32]);
        let cluster = *key.public().as_bytes();
        let signed = SignedRelease::sign(release(&key), &key);
        signed.verify(&cluster).unwrap();

        // Another cluster's key: refused, whoever signed it.
        assert!(signed.verify(other.public().as_bytes()).is_err());

        // A tampered hash breaks the signature.
        let mut forged = signed.clone();
        forged.release.file.sha256 = sha256(b"evil");
        assert!(forged.verify(&cluster).is_err());

        // Signed by someone else but claiming to be this cluster's.
        let impostor = SignedRelease::sign(release(&key), &other);
        assert!(impostor.verify(&cluster).is_err());
    }

    /// A fixed test vector: the encoding and signature must not change by
    /// accident, or agents of different builds would reject each other's
    /// releases.
    #[test]
    fn signed_release_vector() {
        let key = SecretKey::from_bytes(&[7; 32]);
        let signed = SignedRelease::sign(release(&key), &key);
        assert_eq!(
            hex(&sha256(&release_message(&signed.release))),
            "c70143734d4e468cd3ef0b3ee2c31323d04bbda658e6042af062cc716f15a47e"
        );
        assert_eq!(
            hex(&signed.sig.to_bytes()),
            "96edeb7012906c3087b05c9b73db10209a20c61be32c76381a16fd48154e55209a0d524e6b5f11b6a55d3d1f7f616ca574e51fbcea62855eb96ba439bb0d9e0a"
        );
        signed.verify(key.public().as_bytes()).unwrap();
    }

    #[test]
    fn reads_build_info_markers() {
        let mut bin = b"\0\0pumasi-build-info:v=junk\xff;p=1;".to_vec();
        bin.extend(b"pumasi-build-info:v=");
        bin.extend(b"\x01\x02");
        bin.extend(b"pumasi-build-info:v=0.4.0+ci;p=5;rest");
        assert_eq!(
            read_build_info(&bin),
            Some(BuildInfo {
                version: "0.4.0+ci".into(),
                protocol: 5
            })
        );
        assert_eq!(read_build_info(b"no marker here"), None);
        // This crate's own marker.
        let own = read_build_info(crate::build_info().as_bytes()).unwrap();
        assert_eq!(own.version, crate::version());
        assert_eq!(own.protocol, crate::PROTOCOL_VERSION);
    }

    #[test]
    fn detects_targets_from_headers() {
        let mut elf = vec![0u8; 64];
        elf[..4].copy_from_slice(b"\x7fELF");
        elf[4] = 2;
        elf[5] = 1;
        elf[18] = 0x3e;
        assert_eq!(detect_target(&elf).unwrap(), Target::new("linux", "x86_64"));
        elf[18] = 0xb7;
        assert_eq!(
            detect_target(&elf).unwrap(),
            Target::new("linux", "aarch64")
        );

        let mut macho = vec![0xcf, 0xfa, 0xed, 0xfe];
        macho.extend(0x0100_000cu32.to_le_bytes());
        assert_eq!(
            detect_target(&macho).unwrap(),
            Target::new("macos", "aarch64")
        );

        let mut pe = vec![0u8; 0x100];
        pe[..2].copy_from_slice(b"MZ");
        pe[0x3c..0x40].copy_from_slice(&0x80u32.to_le_bytes());
        pe[0x80..0x84].copy_from_slice(b"PE\0\0");
        pe[0x84..0x86].copy_from_slice(&0xaa64u16.to_le_bytes());
        assert_eq!(
            detect_target(&pe).unwrap(),
            Target::new("windows", "aarch64")
        );

        assert!(detect_target(b"#!/bin/sh\n").is_err());
        assert!(detect_target(&[0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 2]).is_err());
    }

    #[test]
    fn detects_the_running_binary() {
        let exe = std::env::current_exe().unwrap();
        let bytes = std::fs::read(exe).unwrap();
        assert_eq!(detect_target(&bytes).unwrap(), Target::host());
    }

    #[test]
    fn parses_targets() {
        assert_eq!(
            "linux/x86_64".parse::<Target>().unwrap(),
            Target::new("linux", "x86_64")
        );
        assert_eq!(
            "windows-aarch64".parse::<Target>().unwrap(),
            Target::new("windows", "aarch64")
        );
        assert!("linux".parse::<Target>().is_err());
    }
}
