//! The cluster file store (phase 8): named, versioned datasets that the
//! control plane keeps in an iroh-blobs store, how jobs mount them under
//! `/input`, task outputs too large to travel inline, the storage rates
//! and the layout of a private image kept in the store.
//!
//! A dataset version is an iroh-blobs collection: a hash sequence whose
//! first child names the files and whose other children are the files'
//! BLAKE3 hashes. Everything is verified while it streams and transfers
//! resume from what the receiver already has.

use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    economy::{CuRates, MicroCu},
    job::{JobId, validate_rel_path},
    perf::DiskClass,
    registry::{ContainerConfig, Descriptor},
};

/// A BLAKE3 hash of a blob or collection, as iroh-blobs computes it.
pub type BlobHash = [u8; 32];

/// The prefix of dataset references in job files: `store:corpus@3/a.csv`.
pub const STORE_PREFIX: &str = "store:";

/// Largest output a task may write to `/output` when it goes to the store
/// (inline outputs keep [`crate::job::MAX_OUTPUT_FILES_BYTES`]).
pub const MAX_STORE_OUTPUT_BYTES: u64 = 64 << 30;

/// Seconds in the month storage rates are quoted for (30 days).
pub const MONTH_SECS: u64 = 30 * 24 * 3600;

const GIB: u128 = 1 << 30;

/// Hex form of a blob hash.
pub fn hash_hex(h: &BlobHash) -> String {
    data_encoding::HEXLOWER.encode(h)
}

/// Parses the hex form of a blob hash.
pub fn parse_hash(s: &str) -> Result<BlobHash> {
    let bytes = data_encoding::HEXLOWER_PERMISSIVE
        .decode(s.trim().as_bytes())
        .context("hash must be hex")?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("hash must be 32 bytes"))
}

/// Dataset names: 1 to 64 characters of `a-z`, `0-9`, `.`, `-`, `_`,
/// starting with a letter or digit.
pub fn validate_dataset_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 64,
        "dataset name must be 1 to 64 characters"
    );
    ensure!(
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || ".-_".contains(c)),
        "dataset name may use only a-z, 0-9, '.', '-' and '_': {name:?}"
    );
    ensure!(
        name.starts_with(|c: char| c.is_ascii_alphanumeric()),
        "dataset name must start with a letter or digit: {name:?}"
    );
    Ok(())
}

/// `NAME[@VERSION][/PATH]`: a dataset, one of its versions (the latest
/// when left out), and optionally one file or directory in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreRef {
    pub name: String,
    pub version: Option<u32>,
    pub path: Option<String>,
}

impl FromStr for StoreRef {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let s = s.strip_prefix(STORE_PREFIX).unwrap_or(s);
        let (head, path) = match s.split_once('/') {
            Some((h, p)) => (h, Some(p.trim_end_matches('/').to_string())),
            None => (s, None),
        };
        let (name, version) = match head.split_once('@') {
            Some((n, v)) => (
                n,
                Some(
                    v.parse::<u32>()
                        .with_context(|| format!("dataset version must be a number: {s:?}"))?,
                ),
            ),
            None => (head, None),
        };
        validate_dataset_name(name)?;
        if let Some(p) = &path {
            validate_rel_path(p).with_context(|| format!("bad path in {s:?}"))?;
        }
        Ok(Self {
            name: name.to_string(),
            version,
            path,
        })
    }
}

impl fmt::Display for StoreRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
        if let Some(v) = self.version {
            write!(f, "@{v}")?;
        }
        if let Some(p) = &self.path {
            write!(f, "/{p}")?;
        }
        Ok(())
    }
}

/// Where the data of a mount comes from, as the CLI submits it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MountSource {
    /// A named dataset (latest version unless one is given).
    Dataset { name: String, version: Option<u32> },
    /// A collection the CLI just uploaded (local `files` too large to send
    /// inline), committed with [`crate::msg::AdminOp::StoreCommit`].
    Upload { root: BlobHash },
}

/// A mount as submitted: which data, which part of it, and where under
/// `/input` it goes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountRequest {
    pub source: MountSource,
    /// Only this file, or the files under this directory.
    pub select: Option<String>,
    /// Directory under `/input` (empty: `/input` itself).
    pub at: String,
}

impl MountRequest {
    /// The mount a `store:` reference stands for: the dataset's files under
    /// `/input/<name>/`, keeping their paths.
    pub fn from_ref(r: &StoreRef) -> Self {
        Self {
            source: MountSource::Dataset {
                name: r.name.clone(),
                version: r.version,
            },
            select: r.path.clone(),
            at: r.name.clone(),
        }
    }
}

/// A mount resolved by the control plane: a collection by hash. Agents
/// fetch it from the control plane (or from nodes that have it) into their
/// cache and place the selected files under `/input/<at>/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    pub root: BlobHash,
    pub select: Option<String>,
    pub at: String,
    /// What it was resolved from (`corpus@3`), for messages.
    pub label: String,
    /// Bytes of the whole collection.
    pub size: u64,
}

impl Mount {
    /// Whether the collection entry `name` is part of this mount.
    pub fn selects(&self, name: &str) -> bool {
        match &self.select {
            None => true,
            Some(s) => {
                name == s || (name.starts_with(s.as_str()) && name[s.len()..].starts_with('/'))
            }
        }
    }

    /// Where the entry `name` appears, relative to `/input`.
    pub fn place(&self, name: &str) -> String {
        if self.at.is_empty() {
            name.to_string()
        } else {
            format!("{}/{name}", self.at)
        }
    }
}

/// Where a task's `/output` goes (protocol 8).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputMode {
    /// In the report, capped at [`crate::job::MAX_OUTPUT_FILES_BYTES`]
    /// (what older agents do).
    #[default]
    Inline,
    /// In the report when it fits, else uploaded to the store.
    Auto,
    /// Always uploaded to the store (the job names an output dataset).
    Store,
}

/// Output files a task uploaded to the store: a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredOutput {
    pub root: BlobHash,
    /// Bytes of all files.
    pub size: u64,
    pub files: u32,
}

/// One version of a dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetView {
    pub name: String,
    pub version: u32,
    /// Account that stores it and pays for it.
    pub account: String,
    pub root: BlobHash,
    pub size: u64,
    pub files: u32,
    pub created_unix: u64,
    /// The job whose outputs it holds, for job output datasets.
    pub job: Option<JobId>,
}

/// What an account stores and may store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreUsageView {
    pub account: String,
    /// Bytes of its dataset versions and task outputs (each counted once
    /// per version or task, even where files are shared).
    pub bytes: u64,
    pub quota: Option<u64>,
    pub datasets: u32,
}

/// What `pumasi store get` asks to download.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreTarget {
    Dataset {
        name: String,
        version: Option<u32>,
    },
    /// Every task's stored output of a job, as `task-<index>/...`.
    Job {
        id: JobId,
    },
}

/// One collection to download and the directory it goes to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreItem {
    /// Directory relative to the destination ("" for the destination).
    pub dir: String,
    pub root: BlobHash,
    pub size: u64,
    pub files: u32,
}

/// Storage settings beyond the rate table: what the other disk classes
/// cost and the default quota. `CuRates::storage_gib_month` is the SSD
/// rate (and the rate when the class is unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePolicy {
    pub hdd_gib_month: MicroCu,
    pub nvme_gib_month: MicroCu,
    /// Bytes an account may store unless it has its own quota. `None`:
    /// no limit.
    pub default_quota: Option<u64>,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            hdd_gib_month: 200_000,
            nvme_gib_month: 800_000,
            default_quota: None,
        }
    }
}

impl StoragePolicy {
    /// The storage rate for data on a disk of this class.
    pub fn rate(&self, rates: &CuRates, class: DiskClass) -> MicroCu {
        match class {
            DiskClass::Hdd => self.hdd_gib_month,
            DiskClass::Nvme => self.nvme_gib_month,
            DiskClass::Ssd | DiskClass::Unknown => rates.storage_gib_month,
        }
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.hdd_gib_month >= 0 && self.nvme_gib_month >= 0,
            "storage rates must not be negative"
        );
        Ok(())
    }
}

/// The exact price in micro-CU of keeping `bytes` for `secs` at `rate`
/// per GiB-month (rounded down).
pub fn storage_charge(bytes: u64, secs: u64, rate: MicroCu) -> MicroCu {
    let v = bytes as u128 * secs as u128 * rate.max(0) as u128 / (GIB * MONTH_SECS as u128);
    v.min(i64::MAX as u128) as MicroCu
}

/// The exact price in micro-CU of sending `bytes` out at `rate` per GiB.
pub fn egress_charge(bytes: u64, rate: MicroCu) -> MicroCu {
    let v = bytes as u128 * rate.max(0) as u128 / GIB;
    v.min(i64::MAX as u128) as MicroCu
}

/// Parses a size such as `500M`, `10G`, `1.5TiB` or a bare number of
/// bytes. Units are powers of 1024.
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num.parse().with_context(|| format!("invalid size {s:?}"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        other => bail!("unknown size unit {other:?} in {s:?}"),
    };
    ensure!(num >= 0.0, "size must not be negative");
    Ok((num * mult as f64).round() as u64)
}

/// `1.5 GiB`, `12 KiB`, `800 B`.
pub fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

/// `16G`, `512M`, `1536K`, `100`: the largest unit that is exact, in the
/// form [`parse_size`] reads back.
pub fn format_size_short(bytes: u64) -> String {
    for (unit, shift) in [("T", 40), ("G", 30), ("M", 20), ("K", 10)] {
        if bytes >= 1 << shift && bytes.is_multiple_of(1 << shift) {
            return format!("{}{unit}", bytes >> shift);
        }
    }
    bytes.to_string()
}

// ---------------------------------------------------------------------------
// Private images in the store
// ---------------------------------------------------------------------------

/// A private image the control plane pulled into the store, so nodes get
/// its layers without registry credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBlob {
    /// Collection: [`IMAGE_MANIFEST`], [`IMAGE_CONFIG`], then the layers
    /// as `layer-000`, `layer-001`, ...
    pub root: BlobHash,
    /// Digest of the platform manifest.
    pub digest: String,
    /// The reference the job named.
    pub reference: String,
}

/// Entry of an image collection describing it ([`ImageManifest`] JSON).
pub const IMAGE_MANIFEST: &str = "image.json";
/// Entry of an image collection holding the raw image config.
pub const IMAGE_CONFIG: &str = "config.json";

/// The name of layer `i` in an image collection.
pub fn image_layer_name(i: usize) -> String {
    format!("layer-{i:03}")
}

/// What [`IMAGE_MANIFEST`] says about the image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageManifest {
    pub reference: String,
    pub digest: String,
    pub config_digest: String,
    pub config: ContainerConfig,
    pub layers: Vec<Descriptor>,
}

/// The tag a node's Docker gets for an image loaded from the store.
pub fn docker_tag(digest: &str) -> String {
    let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
    format!("pumasi-image:{}", &hex[..hex.len().min(64)])
}

/// Expands `{shard}`, `{shards}` and zero-padded `{shard:0N}` in a job
/// file string.
pub fn expand_shard(s: &str, shard: u32, shards: u32) -> String {
    let mut out = s
        .replace("{shard}", &shard.to_string())
        .replace("{shards}", &shards.to_string());
    while let Some(start) = out.find("{shard:0") {
        let Some(len) = out[start..].find('}') else {
            break;
        };
        let width: usize = out[start + 8..start + len].parse().unwrap_or(1);
        out.replace_range(start..start + len + 1, &format!("{shard:0width$}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_refs() {
        let r: StoreRef = "store:corpus@3/dir/a.csv".parse().unwrap();
        assert_eq!(r.name, "corpus");
        assert_eq!(r.version, Some(3));
        assert_eq!(r.path.as_deref(), Some("dir/a.csv"));
        assert_eq!(r.to_string(), "corpus@3/dir/a.csv");
        let r: StoreRef = "corpus".parse().unwrap();
        assert_eq!((r.version, r.path), (None, None));
        let r: StoreRef = "store:corpus/sub/".parse().unwrap();
        assert_eq!(r.path.as_deref(), Some("sub"));
        for bad in ["", "Upper", "a@x", "a/../b", "-x", "a b", "store:/x"] {
            assert!(bad.parse::<StoreRef>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn mounts_select_and_place() {
        let m = Mount {
            root: [0; 32],
            select: Some("parts".into()),
            at: "corpus".into(),
            label: "corpus@1".into(),
            size: 0,
        };
        assert!(m.selects("parts/a.txt"));
        assert!(m.selects("parts"));
        assert!(!m.selects("partsx/a.txt"));
        assert!(!m.selects("other"));
        assert_eq!(m.place("parts/a.txt"), "corpus/parts/a.txt");
        let root = Mount {
            select: None,
            at: String::new(),
            ..m
        };
        assert!(root.selects("x"));
        assert_eq!(root.place("x/y"), "x/y");
    }

    #[test]
    fn sizes_and_charges() {
        assert_eq!(parse_size("10G").unwrap(), 10 << 30);
        assert_eq!(parse_size("1.5GiB").unwrap(), 3 << 29);
        assert_eq!(parse_size("512").unwrap(), 512);
        assert_eq!(parse_size("2 MiB").unwrap(), 2 << 20);
        assert!(parse_size("3 parsecs").is_err());
        assert_eq!(format_size(3 << 29), "1.5 GiB");
        assert_eq!(format_size(800), "800 B");
        // 1 GiB for a month at 0.5 CU.
        assert_eq!(storage_charge(1 << 30, MONTH_SECS, 500_000), 500_000);
        // 10 GiB for an hour: 10 * 0.5 / 720 CU.
        assert_eq!(storage_charge(10 << 30, 3600, 500_000), 6944);
        assert_eq!(egress_charge(2 << 30, 100_000), 200_000);
        let p = StoragePolicy::default();
        let rates = CuRates::default();
        assert_eq!(p.rate(&rates, DiskClass::Hdd), 200_000);
        assert_eq!(p.rate(&rates, DiskClass::Unknown), rates.storage_gib_month);
    }

    #[test]
    fn shard_placeholders() {
        assert_eq!(expand_shard("part-{shard}", 3, 16), "part-3");
        assert_eq!(
            expand_shard("part-{shard:03}-of-{shards}", 3, 16),
            "part-003-of-16"
        );
        assert_eq!(
            docker_tag(&format!("sha256:{}", "ab".repeat(32))).len(),
            13 + 64
        );
        assert_eq!(
            parse_hash(&hash_hex(&[7; 32])).unwrap(),
            [7; 32],
            "hex roundtrip"
        );
    }
}
