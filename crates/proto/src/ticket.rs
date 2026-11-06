//! Tickets: copy-pasteable strings that carry a cluster address plus a
//! secret. A join ticket lets a new node join; an admin ticket lets the CLI
//! manage the cluster from any machine; a replica ticket lets another
//! machine become a control plane replica (phase 13).
//!
//! `cluster` is the cluster's key (the endpoint id of the control plane
//! that created the cluster, which signs releases and receipts) with that
//! machine's addresses. A cluster with replicas also lists every control
//! plane replica in `replicas`; clients try them in turn and follow the
//! one that answers to the leader. Tickets without replicas keep the
//! phase 1 layout, and older binaries read the `cluster` of newer tickets
//! (postcard leaves the trailing list alone).

use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::Secret;

const JOIN_PREFIX: &str = "pumasijoin";
const ADMIN_PREFIX: &str = "pumasiadmin";
const REPLICA_PREFIX: &str = "pumasireplica";
const SIGNIN_PREFIX: &str = "pumasisignin";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Payload {
    cluster: EndpointAddr,
    secret: Secret,
}

/// Phase 13 layout: the control plane replicas follow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PayloadV2 {
    cluster: EndpointAddr,
    secret: Secret,
    replicas: Vec<EndpointAddr>,
}

fn encode(
    prefix: &str,
    cluster: &EndpointAddr,
    secret: &Secret,
    replicas: &[EndpointAddr],
) -> String {
    let bytes = if replicas.is_empty() {
        postcard::to_stdvec(&Payload {
            cluster: cluster.clone(),
            secret: *secret,
        })
    } else {
        postcard::to_stdvec(&PayloadV2 {
            cluster: cluster.clone(),
            secret: *secret,
            replicas: replicas.to_vec(),
        })
    }
    .expect("ticket encodes");
    format!(
        "{prefix}{}",
        data_encoding::BASE32_NOPAD
            .encode(&bytes)
            .to_ascii_lowercase()
    )
}

fn decode(prefix: &str, s: &str) -> Result<PayloadV2> {
    let s = s.trim();
    let Some(body) = s.strip_prefix(prefix) else {
        bail!("not a {prefix} ticket");
    };
    let bytes = data_encoding::BASE32_NOPAD
        .decode(body.to_ascii_uppercase().as_bytes())
        .context("ticket is not valid base32")?;
    // A phase 1 payload is a prefix of the phase 13 one: if the list is
    // not there (or does not decode), it is the old layout.
    match postcard::take_from_bytes::<PayloadV2>(&bytes) {
        Ok((p, [])) => Ok(p),
        _ => {
            let p: Payload = postcard::from_bytes(&bytes).context("ticket payload is corrupt")?;
            Ok(PayloadV2 {
                cluster: p.cluster,
                secret: p.secret,
                replicas: Vec::new(),
            })
        }
    }
}

/// Every address worth trying for a cluster: the replicas (when it has
/// them) and the cluster's own, without duplicates.
pub fn candidates(cluster: &EndpointAddr, replicas: &[EndpointAddr]) -> Vec<EndpointAddr> {
    let mut out: Vec<EndpointAddr> = Vec::new();
    for a in std::iter::once(cluster).chain(replicas) {
        match out.iter_mut().find(|o| o.id == a.id) {
            Some(o) => {
                if o.addrs.is_empty() {
                    o.addrs = a.addrs.clone();
                }
            }
            None => out.push(a.clone()),
        }
    }
    out
}

/// Grants one node the right to join a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinTicket {
    pub cluster: EndpointAddr,
    pub secret: Secret,
    /// The control plane replicas (empty for a single control plane).
    pub replicas: Vec<EndpointAddr>,
}

impl fmt::Display for JoinTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(
            JOIN_PREFIX,
            &self.cluster,
            &self.secret,
            &self.replicas,
        ))
    }
}

impl FromStr for JoinTicket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let p = decode(JOIN_PREFIX, s)?;
        Ok(Self {
            cluster: p.cluster,
            secret: p.secret,
            replicas: p.replicas,
        })
    }
}

/// Full administrative access to a cluster. Treat like a password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminTicket {
    pub cluster: EndpointAddr,
    pub secret: Secret,
    /// The control plane replicas (empty for a single control plane).
    pub replicas: Vec<EndpointAddr>,
}

impl fmt::Display for AdminTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(
            ADMIN_PREFIX,
            &self.cluster,
            &self.secret,
            &self.replicas,
        ))
    }
}

impl FromStr for AdminTicket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let p = decode(ADMIN_PREFIX, s)?;
        Ok(Self {
            cluster: p.cluster,
            secret: p.secret,
            replicas: p.replicas,
        })
    }
}

impl JoinTicket {
    /// The addresses to try, see [`candidates`].
    pub fn candidates(&self) -> Vec<EndpointAddr> {
        candidates(&self.cluster, &self.replicas)
    }
}

impl AdminTicket {
    /// The addresses to try, see [`candidates`].
    pub fn candidates(&self) -> Vec<EndpointAddr> {
        candidates(&self.cluster, &self.replicas)
    }
}

/// Lets one machine join the control plane as a replica (phase 13). Valid
/// once, for a limited time, on the leader that made it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaTicket {
    pub cluster: EndpointAddr,
    pub secret: Secret,
    pub replicas: Vec<EndpointAddr>,
}

impl ReplicaTicket {
    /// The addresses to try, see [`candidates`].
    pub fn candidates(&self) -> Vec<EndpointAddr> {
        candidates(&self.cluster, &self.replicas)
    }
}

impl fmt::Display for ReplicaTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(
            REPLICA_PREFIX,
            &self.cluster,
            &self.secret,
            &self.replicas,
        ))
    }
}

impl FromStr for ReplicaTicket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let p = decode(REPLICA_PREFIX, s)?;
        Ok(Self {
            cluster: p.cluster,
            secret: p.secret,
            replicas: p.replicas,
        })
    }
}

/// Where to sign in with SSO (phase 17): the cluster's addresses and no
/// secret, so it can be posted anywhere people of the organization see
/// it. `pumasi login --sso` and `--ldap` take it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignInTicket {
    pub cluster: EndpointAddr,
    pub replicas: Vec<EndpointAddr>,
}

impl fmt::Display for SignInTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(
            SIGNIN_PREFIX,
            &self.cluster,
            &[0; 32],
            &self.replicas,
        ))
    }
}

impl FromStr for SignInTicket {
    type Err = anyhow::Error;
    /// Also takes an admin or access ticket (an expired one is fine): only
    /// its addresses are used.
    fn from_str(s: &str) -> Result<Self> {
        let p = decode(SIGNIN_PREFIX, s).or_else(|_| decode(ADMIN_PREFIX, s))?;
        Ok(Self {
            cluster: p.cluster,
            replicas: p.replicas,
        })
    }
}

impl SignInTicket {
    /// An admin ticket with no valid secret, for the sign-in calls.
    pub fn anonymous(&self) -> AdminTicket {
        AdminTicket {
            cluster: self.cluster.clone(),
            secret: [0; 32],
            replicas: self.replicas.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    #[test]
    fn roundtrip_and_kind_check() {
        let addr = EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr("127.0.0.1:4433".parse().unwrap());
        let join = JoinTicket {
            cluster: addr.clone(),
            secret: crate::new_secret(),
            replicas: vec![],
        };
        let s = join.to_string();
        assert!(s.starts_with("pumasijoin"));
        assert_eq!(s.parse::<JoinTicket>().unwrap(), join);
        assert!(s.parse::<AdminTicket>().is_err());

        let admin = AdminTicket {
            cluster: addr.clone(),
            secret: crate::new_secret(),
            replicas: vec![],
        };
        assert_eq!(admin.to_string().parse::<AdminTicket>().unwrap(), admin);
    }

    #[test]
    fn replicas_roundtrip_and_old_layout_still_reads() {
        let cluster = EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr("127.0.0.1:4433".parse().unwrap());
        let other = EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr("127.0.0.1:4434".parse().unwrap());
        let t = AdminTicket {
            cluster: cluster.clone(),
            secret: crate::new_secret(),
            replicas: vec![cluster.clone(), other.clone()],
        };
        let s = t.to_string();
        assert_eq!(s.parse::<AdminTicket>().unwrap(), t);
        // An older binary decodes the phase 1 payload and ignores the rest.
        let body = data_encoding::BASE32_NOPAD
            .decode(
                s.strip_prefix(ADMIN_PREFIX)
                    .unwrap()
                    .to_ascii_uppercase()
                    .as_bytes(),
            )
            .unwrap();
        let old: Payload = postcard::from_bytes(&body).unwrap();
        assert_eq!(old.cluster, cluster);
        assert_eq!(old.secret, t.secret);
        // Candidates: no duplicates, the cluster first.
        let c = t.candidates();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].id, cluster.id);
        assert_eq!(c[1].id, other.id);
        let r = ReplicaTicket {
            cluster,
            secret: crate::new_secret(),
            replicas: vec![other],
        };
        assert_eq!(r.to_string().parse::<ReplicaTicket>().unwrap(), r);
        assert!(r.to_string().parse::<AdminTicket>().is_err());
    }

    #[test]
    fn sign_in_tickets_carry_no_secret() {
        let cluster = EndpointAddr::new(SecretKey::generate().public());
        let t = SignInTicket {
            cluster: cluster.clone(),
            replicas: vec![],
        };
        let s = t.to_string();
        assert!(s.starts_with("pumasisignin"));
        assert_eq!(s.parse::<SignInTicket>().unwrap(), t);
        assert!(s.parse::<AdminTicket>().is_err());
        assert_eq!(t.anonymous().secret, [0; 32]);
        let admin = AdminTicket {
            cluster,
            secret: crate::new_secret(),
            replicas: vec![],
        };
        assert_eq!(admin.to_string().parse::<SignInTicket>().unwrap(), t);
    }
}
