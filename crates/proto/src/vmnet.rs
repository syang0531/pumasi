//! What a microVM task's network may reach and how fast (1.18), and what
//! it used.
//!
//! A job with `network = true` reaches any public address, as since phase
//! 9. A job with a list (`network = ["pypi.org:443", "*.pythonhosted.org"]`)
//! or a table (`network = { allow = [...], mbps = 20 }`) reaches only what
//! the list names, and a cap holds its traffic to `mbps` megabits a second
//! each way. Private, loopback, link-local and the host's own addresses
//! stay refused whatever the list says: the list narrows the default
//! policy, never widens it.
//!
//! Hosts are matched by the name the guest looked up through the gateway's
//! DNS: an answer for an allowed name opens its addresses (for the ports
//! the rule gives) for as long as the guest runs. Addresses and CIDR
//! ranges match directly.

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// Most entries in a job's allow list.
pub const MAX_RULES: usize = 256;
/// The highest bandwidth cap a job may ask for, in megabits a second.
pub const MAX_MBPS: u32 = 100_000;

/// A microVM task's network beyond on or off (1.18). The default (empty
/// list, no cap) is phase 9's: every public address, as fast as it goes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetSpec {
    /// What the task may reach; empty: every public address.
    pub allow: Vec<NetRule>,
    /// Megabits a second each way (to and from the internet).
    pub mbps: Option<u32>,
}

impl NetSpec {
    pub fn is_empty(&self) -> bool {
        self.allow.is_empty() && self.mbps.is_none()
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.allow.len() <= MAX_RULES,
            "at most {MAX_RULES} network rules"
        );
        if let Some(m) = self.mbps {
            ensure!(
                (1..=MAX_MBPS).contains(&m),
                "network mbps must be 1 to {MAX_MBPS}"
            );
        }
        Ok(())
    }

    /// Bytes a second each way under the cap.
    pub fn bytes_per_sec(&self) -> Option<u64> {
        self.mbps.map(|m| m as u64 * 1_000_000 / 8)
    }

    /// Whether the list lets the guest look up `name` (and so learn its
    /// addresses). Without a list every name resolves.
    pub fn allows_name(&self, name: &str) -> bool {
        self.allow.is_empty() || self.allow.iter().any(|r| r.host.matches_name(name))
    }

    /// The ports a name the guest looked up opens: `None` when no rule
    /// names it, `Some(None)` for every port.
    pub fn ports_for_name(&self, name: &str) -> Option<Option<Vec<Port>>> {
        if self.allow.is_empty() {
            return Some(None);
        }
        let mut ports = Vec::new();
        for r in self.allow.iter().filter(|r| r.host.matches_name(name)) {
            match r.port {
                None => return Some(None),
                Some(p) => ports.push(p),
            }
        }
        (!ports.is_empty()).then_some(Some(ports))
    }

    /// Whether an address rule (an IP or a CIDR range) lets the guest
    /// reach `ip:port`. Names are checked through what DNS answered.
    pub fn allows_addr(&self, ip: IpAddr, port: u16) -> bool {
        self.allow.is_empty()
            || self
                .allow
                .iter()
                .any(|r| r.host.matches_ip(ip) && r.port.is_none_or(|p| p.contains(port)))
    }

    /// Parses the job file's list.
    pub fn parse_list<S: AsRef<str>>(items: &[S]) -> Result<Vec<NetRule>> {
        items.iter().map(|s| s.as_ref().parse()).collect()
    }
}

/// One entry of an allow list: a host and optionally a port or range of
/// ports (`pypi.org:443`, `*.example.com`, `10.0.0.0/8` is refused as
/// private anyway, `[2001:db8::1]:443`, `1.1.1.1:53`, `api.example.com:8000-8100`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetRule {
    pub host: HostPattern,
    pub port: Option<Port>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostPattern {
    /// A name, matched without case; `*.example.com` matches every name
    /// under `example.com` (not the name itself).
    Name(String),
    /// An address or a range of them.
    Cidr(IpAddr, u8),
}

/// A port or an inclusive range of ports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Port {
    pub first: u16,
    pub last: u16,
}

impl Port {
    pub fn contains(&self, port: u16) -> bool {
        (self.first..=self.last).contains(&port)
    }
}

impl HostPattern {
    pub fn matches_name(&self, name: &str) -> bool {
        let HostPattern::Name(p) = self else {
            return false;
        };
        let name = name.trim_end_matches('.').to_ascii_lowercase();
        match p.strip_prefix("*.") {
            Some(suffix) => name.len() > suffix.len() + 1 && name.ends_with(&format!(".{suffix}")),
            None => name == *p,
        }
    }

    pub fn matches_ip(&self, ip: IpAddr) -> bool {
        let HostPattern::Cidr(net, prefix) = self else {
            return false;
        };
        match (net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                let mask = u32::MAX.checked_shl(32 - *prefix as u32).unwrap_or(0);
                u32::from(*n) & mask == u32::from(a) & mask
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                let mask = u128::MAX.checked_shl(128 - *prefix as u32).unwrap_or(0);
                u128::from(*n) & mask == u128::from(a) & mask
            }
            _ => false,
        }
    }
}

fn parse_port(s: &str) -> Result<Port> {
    let (a, b) = s.split_once('-').unwrap_or((s, s));
    let first: u16 = a
        .trim()
        .parse()
        .with_context(|| format!("bad port {s:?}"))?;
    let last: u16 = b
        .trim()
        .parse()
        .with_context(|| format!("bad port {s:?}"))?;
    ensure!(first >= 1 && first <= last, "bad port range {s:?}");
    Ok(Port { first, last })
}

fn valid_name(name: &str) -> bool {
    let labels: Vec<&str> = name.split('.').collect();
    name.len() <= 253
        && labels.len() >= 2
        && labels.iter().all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && l.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                && !l.starts_with('-')
                && !l.ends_with('-')
        })
}

impl FromStr for NetRule {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        ensure!(!s.is_empty(), "empty network rule");
        // `[v6]:port`, `[v6]`, a bare v6 address or range, `host:port`.
        let (host, port) = if let Some(rest) = s.strip_prefix('[') {
            let (h, after) = rest
                .split_once(']')
                .with_context(|| format!("missing ] in {s:?}"))?;
            let port = match after.strip_prefix(':') {
                Some(p) => Some(parse_port(p)?),
                None if after.is_empty() => None,
                None => bail!("bad network rule {s:?}"),
            };
            (h.to_string(), port)
        } else if s.matches(':').count() > 1 {
            (s.to_string(), None)
        } else {
            match s.rsplit_once(':') {
                Some((h, "*")) => (h.to_string(), None),
                Some((h, p)) => (h.to_string(), Some(parse_port(p)?)),
                None => (s.to_string(), None),
            }
        };
        let host = host.to_ascii_lowercase();
        let pattern = if let Some((ip, prefix)) = host.split_once('/') {
            let ip: IpAddr = ip
                .parse()
                .with_context(|| format!("bad address in {s:?}"))?;
            let max = if ip.is_ipv4() { 32 } else { 128 };
            let prefix: u8 = prefix
                .parse()
                .ok()
                .filter(|p| *p <= max)
                .with_context(|| format!("bad prefix length in {s:?}"))?;
            HostPattern::Cidr(ip, prefix)
        } else if let Ok(ip) = host.parse::<IpAddr>() {
            HostPattern::Cidr(ip, if ip.is_ipv4() { 32 } else { 128 })
        } else {
            let bare = host.strip_prefix("*.").unwrap_or(&host);
            ensure!(
                valid_name(bare),
                "{s:?} is not a host name, address or range (use `name:port`, `*.name`, `1.2.3.4`, `1.2.3.0/24`, `[2001:db8::1]:443`)"
            );
            HostPattern::Name(host)
        };
        Ok(NetRule {
            host: pattern,
            port,
        })
    }
}

impl fmt::Display for NetRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v6 = matches!(self.host, HostPattern::Cidr(IpAddr::V6(_), _));
        match &self.host {
            HostPattern::Name(n) => f.write_str(n)?,
            HostPattern::Cidr(ip, p) if v6 && *p == 128 && self.port.is_some() => {
                write!(f, "[{ip}]")?
            }
            HostPattern::Cidr(ip, p) if (ip.is_ipv4() && *p == 32) || *p == 128 => {
                write!(f, "{ip}")?
            }
            HostPattern::Cidr(ip, p) if v6 && self.port.is_some() => write!(f, "[{ip}/{p}]")?,
            HostPattern::Cidr(ip, p) => write!(f, "{ip}/{p}")?,
        }
        match self.port {
            Some(p) if p.first == p.last => write!(f, ":{}", p.first),
            Some(p) => write!(f, ":{}-{}", p.first, p.last),
            None => Ok(()),
        }
    }
}

/// Whether a guest may reach `ip` at all: public unicast. Lists only
/// narrow this.
pub fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => public_v4(v4),
        IpAddr::V6(v6) => public_v6(v6),
    }
}

/// Public unicast IPv4: not RFC 1918, CGNAT, loopback, link-local,
/// multicast, broadcast, documentation, benchmarking or reserved.
pub fn public_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_documentation()
        || o[0] == 0
        // Shared address space (CGNAT, Tailscale), 100.64.0.0/10.
        || (o[0] == 100 && (o[1] & 0xc0) == 64)
        // IETF protocol assignments, 192.0.0.0/24.
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)
        // Benchmarking, 198.18.0.0/15.
        || (o[0] == 198 && (o[1] & 0xfe) == 18)
        // Reserved, 240.0.0.0/4.
        || o[0] >= 240)
}

/// Public unicast IPv6: global unicast (2000::/3) only, without the
/// ranges that carry or stand for something else: Teredo and the IETF
/// block (2001::/23, which holds benchmarking and ORCHID), documentation
/// (2001:db8::/32) and 6to4 (2002::/16, which embeds IPv4 addresses that
/// may be private).
pub fn public_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    (s[0] & 0xe000) == 0x2000
        && !(s[0] == 0x2001 && s[1] < 0x0200)
        && !(s[0] == 0x2001 && s[1] == 0x0db8)
        && s[0] != 0x2002
}

/// What a microVM task's network carried (1.18), in bytes of IP payload
/// above TCP and UDP: what the guest sent out (`tx`) and what came back
/// (`rx`). DNS answered by the gateway itself is not counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetUsage {
    pub tx_bytes: u64,
    pub rx_bytes: u64,
}

impl NetUsage {
    pub fn total(&self) -> u64 {
        self.tx_bytes.saturating_add(self.rx_bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(s: &str) -> NetRule {
        s.parse().unwrap()
    }

    #[test]
    fn rules_parse_and_print() {
        for (s, shown) in [
            ("pypi.org:443", "pypi.org:443"),
            ("PyPI.org", "pypi.org"),
            ("*.pythonhosted.org:443", "*.pythonhosted.org:443"),
            ("1.1.1.1:53", "1.1.1.1:53"),
            ("8.8.0.0/16", "8.8.0.0/16"),
            ("[2606:4700::1111]:443", "[2606:4700::1111]:443"),
            ("2606:4700::/32", "2606:4700::/32"),
            ("api.example.com:8000-8100", "api.example.com:8000-8100"),
            ("example.com:*", "example.com"),
        ] {
            assert_eq!(rule(s).to_string(), shown, "{s}");
            assert_eq!(rule(shown), rule(s), "{shown} round trips");
        }
        for bad in [
            "",
            "localhost",
            "pypi.org:0",
            "pypi.org:99999",
            "pypi.org:443-80",
            "1.2.3.4/33",
            "[2001:db8::1",
            "bad name.com",
            "-x.com",
        ] {
            assert!(bad.parse::<NetRule>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn names_ports_and_ranges_match() {
        let spec = NetSpec {
            allow: NetSpec::parse_list(&[
                "pypi.org:443",
                "*.pythonhosted.org",
                "1.1.1.0/24:53",
                "[2606:4700::1111]:443",
            ])
            .unwrap(),
            mbps: Some(10),
        };
        assert!(spec.allows_name("pypi.org"));
        assert!(spec.allows_name("PYPI.org."));
        assert!(spec.allows_name("files.pythonhosted.org"));
        assert!(!spec.allows_name("pythonhosted.org"), "only names under it");
        assert!(!spec.allows_name("evil.org"));
        assert!(!spec.allows_name("notpypi.org"));
        assert_eq!(
            spec.ports_for_name("pypi.org"),
            Some(Some(vec![Port {
                first: 443,
                last: 443
            }]))
        );
        assert_eq!(spec.ports_for_name("a.pythonhosted.org"), Some(None));
        assert_eq!(spec.ports_for_name("evil.org"), None);
        assert!(spec.allows_addr("1.1.1.1".parse().unwrap(), 53));
        assert!(!spec.allows_addr("1.1.1.1".parse().unwrap(), 443));
        assert!(!spec.allows_addr("1.1.2.1".parse().unwrap(), 53));
        assert!(spec.allows_addr("2606:4700::1111".parse().unwrap(), 443));
        assert!(!spec.allows_addr("2606:4700::1112".parse().unwrap(), 443));
        assert_eq!(spec.bytes_per_sec(), Some(1_250_000));
        // No list: everything (the policy's public check still applies).
        let open = NetSpec::default();
        assert!(open.allows_name("anything.example"));
        assert!(open.allows_addr("9.9.9.9".parse().unwrap(), 1));
        assert!(open.is_empty());
        assert!(
            NetSpec {
                allow: vec![],
                mbps: Some(0)
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn public_addresses() {
        for ip in [
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "127.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.1.2.3",
            "224.0.0.1",
            "255.255.255.255",
            "240.0.0.1",
            "192.0.0.8",
            "198.18.0.1",
            "192.0.2.1",
            "::1",
            "::",
            "fe80::1",
            "fd00::1",
            "ff02::1",
            "::ffff:10.0.0.1",
            "::ffff:1.1.1.1",
            "64:ff9b::a00:1",
            "2001:db8::1",
            "2001::1",
            "2001:2::1",
            "2002:a00:1::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "100.128.0.1",
            "172.32.0.1",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
            "2a00:1450::1",
        ] {
            assert!(public_ip(ip.parse().unwrap()), "{ip}");
        }
    }
}
