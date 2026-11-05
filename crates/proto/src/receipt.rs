//! Usage receipts: what one run of one task used, signed by the node that
//! ran it and countersigned by the control plane that accepted it.
//!
//! The node signs [`Usage`] with its endpoint key (the same ed25519 key its
//! iroh identity is). The control plane checks that signature and the
//! numbers, adds [`Billing`] (who pays whom, how much) and signs the whole
//! thing with its own endpoint key. Anyone holding a [`Receipt`] can check
//! both signatures offline: the public keys are inside it.

use anyhow::{Context, Result, ensure};
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::{
    economy::{MicroCu, MilliCu},
    job::{Executor, FileEntry},
    msg::{Outcome, TaskRef},
};

const NODE_DOMAIN: &[u8] = b"pumasi/receipt/node/v1\0";
const CLUSTER_DOMAIN: &[u8] = b"pumasi/receipt/cluster/v2\0";
/// Receipts of runs with GPUs: the cluster signature also covers the GPU
/// part. Receipts without GPUs keep the v2 message, so they verify the
/// same in older versions.
const CLUSTER_GPU_DOMAIN: &[u8] = b"pumasi/receipt/cluster/v3\0";
const BLOB_PREFIX: &str = "pumasicureceipt";
/// Receipts from before compute units (pumasi 0.6 and older).
const OLD_BLOB_PREFIX: &str = "pumasireceipt";

/// Metered usage of one run, as the node reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Control plane public key.
    pub cluster: [u8; 32],
    /// Node public key.
    pub node: [u8; 32],
    pub task: TaskRef,
    pub executor: Executor,
    pub outcome: Outcome,
    pub fuel_used: u64,
    pub wall_ms: u64,
    /// Memory reserved for the task (its limit).
    pub memory_limit_bytes: u64,
    /// Highest linear memory the task reached (WASM only).
    pub peak_memory_bytes: Option<u64>,
    /// CPU limit in thousandths of a core (Docker only).
    pub cpu_millis: Option<u32>,
    /// [`result_digest`] of what the run produced.
    pub result_digest: [u8; 32],
    pub finished_unix: u64,
}

/// Usage plus the node's signature over it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedUsage {
    pub usage: Usage,
    pub node_sig: Signature,
}

fn node_message(usage: &Usage) -> Vec<u8> {
    let mut m = NODE_DOMAIN.to_vec();
    m.extend(postcard::to_stdvec(usage).expect("usage encodes"));
    m
}

impl SignedUsage {
    pub fn sign(usage: Usage, key: &SecretKey) -> Self {
        let node_sig = key.sign(&node_message(&usage));
        Self { usage, node_sig }
    }

    /// Checks the node signature against the node key inside the usage.
    pub fn verify(&self) -> Result<()> {
        let key = PublicKey::from_bytes(&self.usage.node).context("bad node key")?;
        key.verify(&node_message(&self.usage), &self.node_sig)
            .map_err(|_| anyhow::anyhow!("node signature does not match"))
    }
}

/// What the control plane charged for a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Billing {
    /// Account that submitted the job.
    pub consumer: String,
    /// Account the node belongs to.
    pub provider: String,
    /// Wall time used for pricing: the node's figure, capped by how long
    /// the control plane itself saw the task running.
    pub billed_wall_ms: u64,
    /// CPU score the run was billed at (1000 = the reference vCPU).
    pub cpu_score: u32,
    /// The run's exact price from the cluster's CU rates, in micro-CU.
    /// Zero when consumer and provider are the same account.
    pub exact_ucu: MicroCu,
    /// Milli-CU moved: the exact price plus what earlier runs between the
    /// same accounts left over, in whole milli-CU.
    pub amount: MilliCu,
    /// True for a check run (a replica of a task already run elsewhere).
    pub check: bool,
    pub recorded_unix: u64,
}

/// The GPUs a run had and what they cost (phase 9). The control plane
/// assigned them, so it records them; [`Billing::exact_ucu`] includes
/// `exact_ucu`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuBilling {
    /// Each GPU's name, as the node reported it.
    pub names: Vec<String>,
    /// FP32 GFLOPS of all the GPUs together, as billed.
    pub gflops: u64,
    /// Memory of all the GPUs together.
    pub vram_mib: u64,
    /// The GPU part of the price, in micro-CU.
    pub exact_ucu: MicroCu,
}

/// A countersigned usage receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub usage: Usage,
    pub node_sig: Signature,
    pub billing: Billing,
    pub cluster_sig: Signature,
    /// Present for runs with GPUs. Last, so receipts from before phase 9
    /// decode with [`Receipt::decode`].
    pub gpu: Option<GpuBilling>,
}

/// A receipt from before phase 9.
#[derive(Deserialize)]
struct ReceiptV2 {
    usage: Usage,
    node_sig: Signature,
    billing: Billing,
    cluster_sig: Signature,
}

fn cluster_message(
    usage: &Usage,
    node_sig: &Signature,
    billing: &Billing,
    gpu: Option<&GpuBilling>,
) -> Vec<u8> {
    match gpu {
        None => {
            let mut m = CLUSTER_DOMAIN.to_vec();
            m.extend(postcard::to_stdvec(&(usage, node_sig, billing)).expect("receipt encodes"));
            m
        }
        Some(g) => {
            let mut m = CLUSTER_GPU_DOMAIN.to_vec();
            m.extend(postcard::to_stdvec(&(usage, node_sig, billing, g)).expect("receipt encodes"));
            m
        }
    }
}

impl Receipt {
    /// Countersigns a node's usage. The caller must have checked it.
    pub fn countersign(signed: SignedUsage, billing: Billing, key: &SecretKey) -> Self {
        Self::countersign_gpu(signed, billing, None, key)
    }

    /// [`Self::countersign`] for a run that may have had GPUs.
    pub fn countersign_gpu(
        signed: SignedUsage,
        billing: Billing,
        gpu: Option<GpuBilling>,
        key: &SecretKey,
    ) -> Self {
        let cluster_sig = key.sign(&cluster_message(
            &signed.usage,
            &signed.node_sig,
            &billing,
            gpu.as_ref(),
        ));
        Self {
            usage: signed.usage,
            node_sig: signed.node_sig,
            billing,
            cluster_sig,
            gpu,
        }
    }

    /// Decodes a receipt of this layout or of the one before phase 9.
    pub fn decode(bytes: &[u8]) -> postcard::Result<Self> {
        postcard::from_bytes::<Self>(bytes).or_else(|e| {
            let old: ReceiptV2 = postcard::from_bytes(bytes).map_err(|_| e)?;
            Ok(Self {
                usage: old.usage,
                node_sig: old.node_sig,
                billing: old.billing,
                cluster_sig: old.cluster_sig,
                gpu: None,
            })
        })
    }

    /// Checks both signatures.
    pub fn verify(&self) -> Result<()> {
        SignedUsage {
            usage: self.usage.clone(),
            node_sig: self.node_sig,
        }
        .verify()?;
        let key = PublicKey::from_bytes(&self.usage.cluster).context("bad cluster key")?;
        key.verify(
            &cluster_message(
                &self.usage,
                &self.node_sig,
                &self.billing,
                self.gpu.as_ref(),
            ),
            &self.cluster_sig,
        )
        .map_err(|_| anyhow::anyhow!("control plane signature does not match"))
    }

    /// Stable identifier: the hash of the signed usage.
    pub fn id(&self) -> [u8; 32] {
        *blake3::hash(&node_message(&self.usage)).as_bytes()
    }

    /// Short hex form of [`Self::id`].
    pub fn short_id(&self) -> String {
        crate::short_hex(&self.id())
    }

    pub fn node_id(&self) -> String {
        PublicKey::from_bytes(&self.usage.node)
            .map(|k| k.to_string())
            .unwrap_or_default()
    }

    pub fn cluster_id(&self) -> String {
        PublicKey::from_bytes(&self.usage.cluster)
            .map(|k| k.to_string())
            .unwrap_or_default()
    }

    /// Exact, copy-pasteable encoding used in exports.
    pub fn to_blob(&self) -> String {
        let bytes = postcard::to_stdvec(self).expect("receipt encodes");
        format!(
            "{BLOB_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&bytes)
                .to_ascii_lowercase()
        )
    }

    pub fn from_blob(s: &str) -> Result<Self> {
        let s = s.trim();
        if !s.starts_with(BLOB_PREFIX) && s.starts_with(OLD_BLOB_PREFIX) {
            anyhow::bail!(
                "a receipt from before compute units (pumasi 0.6 or older); check it with that version"
            );
        }
        let body = s
            .strip_prefix(BLOB_PREFIX)
            .context("not a pumasi receipt")?;
        let bytes = data_encoding::BASE32_NOPAD
            .decode(body.to_ascii_uppercase().as_bytes())
            .context("receipt is not valid base32")?;
        Self::decode(&bytes).context("receipt is corrupt")
    }
}

/// Hash of what a run produced: its outcome, stdout and output files. Two
/// honest runs of a deterministic task give the same digest. stderr is left
/// out because the agent may append its own notes to it.
pub fn result_digest(outcome: &Outcome, stdout: &[u8], files: &[FileEntry]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/result/v1\0");
    let outcome = postcard::to_stdvec(outcome).expect("outcome encodes");
    for part in [outcome.as_slice(), stdout] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    let mut files: Vec<&FileEntry> = files.iter().collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    h.update(&(files.len() as u64).to_le_bytes());
    for f in files {
        h.update(&(f.path.len() as u64).to_le_bytes());
        h.update(f.path.as_bytes());
        h.update(&(f.data.len() as u64).to_le_bytes());
        h.update(&f.data);
    }
    *h.finalize().as_bytes()
}

/// [`result_digest`] for a run whose output files may also be in the
/// file store (protocol 8): the same digest when nothing was stored, so
/// older agents and inline results are unchanged.
pub fn output_digest(
    outcome: &Outcome,
    stdout: &[u8],
    files: &[FileEntry],
    stored: Option<&crate::store::StoredOutput>,
) -> [u8; 32] {
    let base = result_digest(outcome, stdout, files);
    let Some(s) = stored else {
        return base;
    };
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/result/v2\0");
    h.update(&base);
    h.update(&s.root);
    h.update(&s.size.to_le_bytes());
    h.update(&s.files.to_le_bytes());
    *h.finalize().as_bytes()
}

/// Checks that a node's usage describes the run it claims to.
pub fn check_usage(
    signed: &SignedUsage,
    node: &PublicKey,
    cluster: &PublicKey,
    task: TaskRef,
    digest: &[u8; 32],
) -> Result<()> {
    signed.verify()?;
    let u = &signed.usage;
    ensure!(&u.node == node.as_bytes(), "receipt names another node");
    ensure!(
        &u.cluster == cluster.as_bytes(),
        "receipt names another cluster"
    );
    ensure!(u.task == task, "receipt is for another task");
    ensure!(
        &u.result_digest == digest,
        "receipt does not match the result"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobId;

    fn usage(node: &SecretKey, cluster: &SecretKey) -> Usage {
        Usage {
            cluster: *cluster.public().as_bytes(),
            node: *node.public().as_bytes(),
            task: TaskRef {
                job: JobId(7),
                index: 1,
                attempt: 1,
                check: false,
            },
            executor: Executor::Wasm,
            outcome: Outcome::Exited(0),
            fuel_used: 1234,
            wall_ms: 10,
            memory_limit_bytes: 1 << 20,
            peak_memory_bytes: Some(65536),
            cpu_millis: None,
            result_digest: result_digest(&Outcome::Exited(0), b"hi", &[]),
            finished_unix: 1,
        }
    }

    #[test]
    fn sign_countersign_verify() {
        let node = SecretKey::generate();
        let cluster = SecretKey::generate();
        let signed = SignedUsage::sign(usage(&node, &cluster), &node);
        signed.verify().unwrap();
        let digest = result_digest(&Outcome::Exited(0), b"hi", &[]);
        check_usage(
            &signed,
            &node.public(),
            &cluster.public(),
            signed.usage.task,
            &digest,
        )
        .unwrap();
        assert!(
            check_usage(
                &signed,
                &cluster.public(),
                &cluster.public(),
                signed.usage.task,
                &digest
            )
            .is_err()
        );

        let billing = Billing {
            consumer: "owner".into(),
            provider: "bob".into(),
            billed_wall_ms: 10,
            cpu_score: 1000,
            exact_ucu: 5_400,
            amount: 5,
            check: false,
            recorded_unix: 2,
        };
        let receipt = Receipt::countersign(signed, billing, &cluster);
        receipt.verify().unwrap();
        let back = Receipt::from_blob(&receipt.to_blob()).unwrap();
        assert_eq!(back, receipt);
        assert!(Receipt::from_blob("pumasireceiptaaaa").is_err());
        assert_eq!(receipt.node_id(), node.public().to_string());

        // Any change breaks a signature.
        let mut forged = receipt.clone();
        forged.billing.amount = 500;
        assert!(forged.verify().is_err());
        let mut forged = receipt.clone();
        forged.usage.fuel_used += 1;
        assert!(forged.verify().is_err());

        // A receipt from before phase 9 (no GPU part) still decodes and
        // verifies: its signature is over the same message.
        #[derive(Serialize)]
        struct V2<'a>(&'a Usage, &'a Signature, &'a Billing, &'a Signature);
        let old = postcard::to_stdvec(&V2(
            &receipt.usage,
            &receipt.node_sig,
            &receipt.billing,
            &receipt.cluster_sig,
        ))
        .unwrap();
        let back = Receipt::decode(&old).unwrap();
        assert_eq!(back, receipt);
        back.verify().unwrap();

        // With GPUs, the GPU part is signed too.
        let signed = SignedUsage::sign(usage(&node, &cluster), &node);
        let gpu = GpuBilling {
            names: vec!["NVIDIA GeForce RTX 3060".into()],
            gflops: 12_700,
            vram_mib: 12 << 10,
            exact_ucu: 4_870_000,
        };
        let r = Receipt::countersign_gpu(signed, receipt.billing.clone(), Some(gpu), &cluster);
        r.verify().unwrap();
        assert_eq!(Receipt::from_blob(&r.to_blob()).unwrap(), r);
        let mut forged = r.clone();
        forged.gpu.as_mut().unwrap().gflops = 1;
        assert!(forged.verify().is_err());
        let mut stripped = r.clone();
        stripped.gpu = None;
        assert!(stripped.verify().is_err());
    }

    #[test]
    fn digest_covers_outcome_stdout_and_files() {
        let f = |p: &str, d: &[u8]| FileEntry {
            path: p.into(),
            data: d.to_vec(),
        };
        let base = result_digest(&Outcome::Exited(0), b"x", &[f("a", b"1"), f("b", b"2")]);
        assert_eq!(
            base,
            result_digest(&Outcome::Exited(0), b"x", &[f("b", b"2"), f("a", b"1")]),
            "file order does not matter"
        );
        assert_ne!(
            base,
            result_digest(&Outcome::Exited(1), b"x", &[f("a", b"1"), f("b", b"2")])
        );
        assert_ne!(
            base,
            result_digest(&Outcome::Exited(0), b"y", &[f("a", b"1"), f("b", b"2")])
        );
        assert_ne!(
            base,
            result_digest(&Outcome::Exited(0), b"x", &[f("a", b"1"), f("b", b"3")])
        );
        assert_ne!(
            base,
            result_digest(&Outcome::Exited(0), b"x", &[f("a", b"1")])
        );
    }
}
