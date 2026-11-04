//! Tickets: copy-pasteable strings that carry a cluster address plus a
//! secret. A join ticket lets a new node join; an admin ticket lets the CLI
//! manage the cluster from any machine.

use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::Secret;

const JOIN_PREFIX: &str = "pumasijoin";
const ADMIN_PREFIX: &str = "pumasiadmin";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Payload {
    cluster: EndpointAddr,
    secret: Secret,
}

fn encode(prefix: &str, cluster: &EndpointAddr, secret: &Secret) -> String {
    let payload = Payload {
        cluster: cluster.clone(),
        secret: *secret,
    };
    let bytes = postcard::to_stdvec(&payload).expect("ticket encodes");
    format!(
        "{prefix}{}",
        data_encoding::BASE32_NOPAD
            .encode(&bytes)
            .to_ascii_lowercase()
    )
}

fn decode(prefix: &str, s: &str) -> Result<Payload> {
    let s = s.trim();
    let Some(body) = s.strip_prefix(prefix) else {
        bail!("not a {prefix} ticket");
    };
    let bytes = data_encoding::BASE32_NOPAD
        .decode(body.to_ascii_uppercase().as_bytes())
        .context("ticket is not valid base32")?;
    postcard::from_bytes(&bytes).context("ticket payload is corrupt")
}

/// Grants one node the right to join a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinTicket {
    pub cluster: EndpointAddr,
    pub secret: Secret,
}

impl fmt::Display for JoinTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(JOIN_PREFIX, &self.cluster, &self.secret))
    }
}

impl FromStr for JoinTicket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let p = decode(JOIN_PREFIX, s)?;
        Ok(Self {
            cluster: p.cluster,
            secret: p.secret,
        })
    }
}

/// Full administrative access to a cluster. Treat like a password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminTicket {
    pub cluster: EndpointAddr,
    pub secret: Secret,
}

impl fmt::Display for AdminTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(ADMIN_PREFIX, &self.cluster, &self.secret))
    }
}

impl FromStr for AdminTicket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let p = decode(ADMIN_PREFIX, s)?;
        Ok(Self {
            cluster: p.cluster,
            secret: p.secret,
        })
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
        };
        let s = join.to_string();
        assert!(s.starts_with("pumasijoin"));
        assert_eq!(s.parse::<JoinTicket>().unwrap(), join);
        assert!(s.parse::<AdminTicket>().is_err());

        let admin = AdminTicket {
            cluster: addr,
            secret: crate::new_secret(),
        };
        assert_eq!(admin.to_string().parse::<AdminTicket>().unwrap(), admin);
    }
}
