//! Machines that sleep (1.5): what an agent tells the control plane about
//! its machine's sleep and its LAN, and the Wake-on-LAN magic packet.
//!
//! Home machines sleep. An agent holds the OS's sleep blocker while it
//! runs tasks, tells the control plane when the OS announces sleep (so its
//! runs move at once and the node shows as asleep, not offline), and, when
//! its owner opted in, can be woken by another machine on its LAN sending
//! the magic packet to its network card.

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

/// A MAC address.
pub type Mac = [u8; 6];

/// `aa:bb:cc:dd:ee:ff`.
pub fn format_mac(mac: &Mac) -> String {
    mac.iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Parses `aa:bb:cc:dd:ee:ff` or `AA-BB-CC-DD-EE-FF`.
pub fn parse_mac(s: &str) -> Result<Mac> {
    let parts: Vec<&str> = s.trim().split([':', '-']).collect();
    ensure!(parts.len() == 6, "{s:?} is not a MAC address");
    let mut mac = [0u8; 6];
    for (b, p) in mac.iter_mut().zip(parts) {
        ensure!(p.len() == 2, "{s:?} is not a MAC address");
        *b = u8::from_str_radix(p, 16).with_context(|| format!("{s:?} is not a MAC address"))?;
    }
    Ok(mac)
}

/// One network interface of a machine that reaches its LAN over IPv4.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanIface {
    pub name: String,
    pub mac: Mac,
    pub addr: [u8; 4],
    /// Length of the subnet's prefix (24 for 255.255.255.0).
    pub prefix: u8,
    /// Looks like a Wi-Fi card. Most wake only over a cable, so wired
    /// cards are tried first; the packet goes to these too.
    pub wireless: bool,
}

impl LanIface {
    pub fn addr(&self) -> Ipv4Addr {
        Ipv4Addr::from(self.addr)
    }

    /// The subnet's network address and prefix length.
    pub fn subnet(&self) -> (u32, u8) {
        (u32::from(self.addr()) & mask(self.prefix), self.prefix)
    }

    /// The subnet's broadcast address.
    pub fn broadcast(&self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.addr()) | !mask(self.prefix))
    }

    /// Whether `other` is on the same subnet (the same network address
    /// under the shorter of the two prefixes, so a card that reports /16
    /// still matches one that reports /24 of the same LAN).
    pub fn same_lan(&self, other: &LanIface) -> bool {
        let m = mask(self.prefix.min(other.prefix));
        u32::from(self.addr()) & m == u32::from(other.addr()) & m
    }

    /// `192.168.0.12/24`.
    pub fn cidr(&self) -> String {
        format!("{}/{}", self.addr(), self.prefix)
    }
}

fn mask(prefix: u8) -> u32 {
    match prefix.min(32) {
        0 => 0,
        p => u32::MAX << (32 - p),
    }
}

/// What a paused node does with the tasks it is running (`--on-pause`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnPause {
    /// They finish; the node only takes no new ones (the default).
    #[default]
    Finish,
    /// They stop and run on another node.
    Requeue,
}

impl std::str::FromStr for OnPause {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.trim() {
            "finish" => Ok(OnPause::Finish),
            "requeue" => Ok(OnPause::Requeue),
            other => anyhow::bail!("unknown --on-pause {other:?} (finish or requeue)"),
        }
    }
}

impl std::fmt::Display for OnPause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            OnPause::Finish => "finish",
            OnPause::Requeue => "requeue",
        })
    }
}

/// How a node's machine sleeps and wakes, sent after the welcome
/// ([`crate::msg::FromAgent::Power`]). Fields are only ever added at the
/// end.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerReport {
    /// It keeps the machine awake while it runs tasks.
    pub stays_awake: bool,
    /// It tells the control plane before the machine sleeps.
    pub sleep_notice: bool,
    /// Its owner lets the cluster wake it over the LAN (`--wake-on-lan`).
    pub wake_on_lan: bool,
    /// Its LAN interfaces: where to send its magic packet, and which
    /// other nodes' it can send.
    pub lan: Vec<LanIface>,
    pub on_pause: OnPause,
    /// Why it does not stay awake or cannot tell before it sleeps, when
    /// its owner did not turn that off.
    pub notes: Vec<String>,
}

/// A node's sleep and wake state in the admin API
/// ([`crate::msg::AdminOp::NodePower`]). Fields are only ever added at the
/// end.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePowerView {
    pub id: String,
    /// Set while the node is asleep: when it said so.
    pub asleep_since_unix: Option<u64>,
    /// Its latest report (kept while it is offline). `None` for agents
    /// older than 1.5.
    pub report: Option<PowerReport>,
    /// When the cluster last sent its magic packet, and through which
    /// machine.
    pub last_wake_unix: Option<u64>,
    pub last_wake_by: Option<String>,
    /// Whether some machine could send its magic packet right now.
    pub can_wake: bool,
}

/// UDP port magic packets go to (discard; network cards look only at the
/// payload).
pub const WAKE_PORT: u16 = 9;

/// The Wake-on-LAN magic packet for `mac`: six `0xff` bytes, then the MAC
/// address sixteen times.
pub fn magic_packet(mac: &Mac) -> [u8; 102] {
    let mut p = [0xffu8; 102];
    for i in 0..16 {
        p[6 + i * 6..12 + i * 6].copy_from_slice(mac);
    }
    p
}

/// Sends `mac`'s magic packet from `from` (an address of the interface on
/// the target's LAN) to that subnet's broadcast address and to the limited
/// broadcast address. Three times each: UDP may drop one, and a card that
/// is just falling asleep may miss the first.
pub fn send_magic_packet(mac: &Mac, from: Ipv4Addr, broadcast: Ipv4Addr) -> Result<()> {
    let socket = UdpSocket::bind(SocketAddrV4::new(from, 0))
        .with_context(|| format!("bind a UDP socket on {from}"))?;
    socket.set_broadcast(true).context("allow broadcast")?;
    let packet = magic_packet(mac);
    let mut sent = false;
    let mut last = None;
    for _ in 0..3 {
        for to in [broadcast, Ipv4Addr::BROADCAST] {
            match socket.send_to(&packet, SocketAddrV4::new(to, WAKE_PORT)) {
                Ok(_) => sent = true,
                Err(e) => last = Some((to, e)),
            }
        }
    }
    match (sent, last) {
        (true, _) => Ok(()),
        (false, Some((to, e))) => Err(e).with_context(|| format!("send the magic packet to {to}")),
        (false, None) => Ok(()),
    }
}

/// Sends magic packets from this machine. [`SystemWake`] is the real
/// one; tests use their own.
pub trait WakeSender: Send + Sync {
    /// This machine's LAN interfaces.
    fn lan(&self) -> Vec<LanIface>;
    /// Sends `mac`'s magic packet out of interface `from`.
    fn send(&self, mac: &Mac, from: &LanIface) -> Result<()>;
}

/// This machine's network cards.
pub struct SystemWake;

impl WakeSender for SystemWake {
    fn lan(&self) -> Vec<LanIface> {
        lan_ifaces()
    }

    fn send(&self, mac: &Mac, from: &LanIface) -> Result<()> {
        send_magic_packet(mac, from.addr(), from.broadcast())
    }
}

/// Sends the magic packet of each of `macs` out of every LAN interface of
/// this machine. Returns how many packets went out and the errors.
pub fn wake_all(sender: &dyn WakeSender, macs: &[Mac]) -> (u32, Vec<String>) {
    let mut sent = 0;
    let mut errors = Vec::new();
    for from in sender.lan() {
        for mac in macs {
            match sender.send(mac, &from) {
                Ok(()) => sent += 1,
                Err(e) => errors.push(format!("{} from {}: {e:#}", format_mac(mac), from.cidr())),
            }
        }
    }
    (sent, errors)
}

/// Interface names that are never a LAN card: loopback, tunnels, bridges
/// and the virtual switches of Docker, VMs and WSL.
fn is_virtual(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    [
        "lo",
        "docker",
        "veth",
        "br-",
        "virbr",
        "tun",
        "tap",
        "utun",
        "awdl",
        "llw",
        "bridge",
        "vmnet",
        "vethernet",
        "loopback",
        "zt",
        "tailscale",
        "wg",
        "gif",
        "stf",
        "anpi",
        "ap",
        "vboxnet",
        "podman",
        "cni",
        "flannel",
        "isatap",
        "teredo",
    ]
    .iter()
    .any(|p| n.starts_with(p))
        || n.contains("virtual")
        || n.contains("hyper-v")
        || n.contains("wsl")
        || n.contains("bluetooth")
}

/// Names of this machine's Wi-Fi interfaces that the name alone does not
/// give away (macOS calls both kinds `enN`).
fn wireless_names() -> Vec<String> {
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("networksetup")
            .arg("-listallhardwareports")
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output();
        if let Ok(out) = out {
            return parse_hardware_ports(&String::from_utf8_lossy(&out.stdout));
        }
    }
    Vec::new()
}

/// `networksetup -listallhardwareports`: the devices of the Wi-Fi (or
/// AirPort) hardware ports.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn parse_hardware_ports(out: &str) -> Vec<String> {
    let mut wifi = false;
    let mut names = Vec::new();
    for line in out.lines() {
        if let Some(port) = line.strip_prefix("Hardware Port:") {
            let port = port.trim().to_ascii_lowercase();
            wifi = port.contains("wi-fi") || port.contains("airport");
        } else if let Some(dev) = line.strip_prefix("Device:")
            && wifi
        {
            names.push(dev.trim().to_string());
        }
    }
    names
}

/// This machine's LAN interfaces: every real network card with a MAC
/// address and a private or public IPv4 address, wired ones first.
pub fn lan_ifaces() -> Vec<LanIface> {
    let wifi = wireless_names();
    let networks = sysinfo::Networks::new_with_refreshed_list();
    let mut out = Vec::new();
    for (name, data) in &networks {
        let mac = data.mac_address().0;
        if is_virtual(name) || mac == [0; 6] {
            continue;
        }
        let lower = name.to_ascii_lowercase();
        let wireless = wifi.iter().any(|w| w == name)
            || lower.starts_with("wl")
            || ["wi-fi", "wifi", "wlan", "wireless", "무선"]
                .iter()
                .any(|w| lower.contains(w))
            || std::path::Path::new("/sys/class/net")
                .join(name)
                .join("wireless")
                .exists();
        for net in data.ip_networks() {
            let std::net::IpAddr::V4(addr) = net.addr else {
                continue;
            };
            if addr.is_loopback() || addr.is_link_local() || addr.is_unspecified() {
                continue;
            }
            out.push(LanIface {
                name: name.clone(),
                mac,
                addr: addr.octets(),
                prefix: net.prefix.min(32),
                wireless,
            });
        }
    }
    out.sort_by(|a, b| (a.wireless, &a.name, a.addr).cmp(&(b.wireless, &b.name, b.addr)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface(addr: [u8; 4], prefix: u8) -> LanIface {
        LanIface {
            name: "en0".into(),
            mac: [0, 1, 2, 3, 4, 5],
            addr,
            prefix,
            wireless: false,
        }
    }

    #[test]
    fn magic_packet_is_six_ff_then_the_mac_sixteen_times() {
        let mac = [0x00, 0x11, 0x22, 0xaa, 0xbb, 0xcc];
        let p = magic_packet(&mac);
        assert_eq!(p.len(), 102);
        assert_eq!(&p[..6], &[0xff; 6]);
        for i in 0..16 {
            assert_eq!(&p[6 + i * 6..12 + i * 6], &mac, "repeat {i}");
        }
    }

    #[test]
    fn mac_addresses_parse_and_print() {
        let mac = parse_mac("00:11:22:AA:bb:cc").unwrap();
        assert_eq!(mac, [0x00, 0x11, 0x22, 0xaa, 0xbb, 0xcc]);
        assert_eq!(format_mac(&mac), "00:11:22:aa:bb:cc");
        assert_eq!(parse_mac("00-11-22-aa-bb-cc").unwrap(), mac);
        for bad in [
            "",
            "00:11:22:aa:bb",
            "00:11:22:aa:bb:cc:dd",
            "0:11:22:aa:bb:cc",
            "zz:11:22:aa:bb:cc",
        ] {
            assert!(parse_mac(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn lan_interfaces_skip_virtual_ones() {
        for name in [
            "lo",
            "docker0",
            "veth12ab",
            "br-0f3",
            "utun3",
            "vEthernet (WSL)",
            "awdl0",
        ] {
            assert!(is_virtual(name), "{name}");
        }
        for name in [
            "eth0",
            "enp3s0",
            "en0",
            "wlan0",
            "Ethernet",
            "이더넷",
            "Wi-Fi",
        ] {
            assert!(!is_virtual(name), "{name}");
        }
        let ports = "\nHardware Port: Ethernet\nDevice: en0\nEthernet Address: 00:11:22:33:44:55\n\nHardware Port: Wi-Fi\nDevice: en1\nEthernet Address: 66:77:88:99:aa:bb\n\nHardware Port: Thunderbolt 1\nDevice: en2\n";
        assert_eq!(parse_hardware_ports(ports), vec!["en1".to_string()]);
        // Reading the real machine never fails, and lists no loopback.
        assert!(lan_ifaces().iter().all(|i| !i.addr().is_loopback()));
    }

    #[test]
    fn subnets_and_broadcast_addresses() {
        let a = iface([192, 168, 0, 12], 24);
        assert_eq!(a.broadcast(), Ipv4Addr::new(192, 168, 0, 255));
        assert_eq!(a.cidr(), "192.168.0.12/24");
        assert!(a.same_lan(&iface([192, 168, 0, 200], 24)));
        assert!(!a.same_lan(&iface([192, 168, 1, 200], 24)));
        // One side reports a wider subnet of the same LAN.
        assert!(a.same_lan(&iface([192, 168, 0, 7], 16)));
        assert_eq!(
            iface([10, 1, 2, 3], 8).broadcast(),
            Ipv4Addr::new(10, 255, 255, 255)
        );
        assert_eq!(
            iface([172, 16, 5, 9], 20).broadcast(),
            Ipv4Addr::new(172, 16, 15, 255)
        );
        assert_eq!(
            iface([1, 2, 3, 4], 32).broadcast(),
            Ipv4Addr::new(1, 2, 3, 4)
        );
        assert_eq!(iface([1, 2, 3, 4], 0).subnet(), (0, 0));
    }

    #[test]
    fn the_packet_arrives_on_the_loopback() {
        // Broadcast needs a real LAN; on the loopback the same code path
        // sends to an address we listen on.
        let listener = UdpSocket::bind("127.0.0.1:0").unwrap();
        listener
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let mac = [1, 2, 3, 4, 5, 6];
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .send_to(&magic_packet(&mac), ("127.0.0.1", port))
            .unwrap();
        let mut buf = [0u8; 200];
        let (n, _) = listener.recv_from(&mut buf).unwrap();
        assert_eq!(&buf[..n], &magic_packet(&mac));
        assert_eq!("requeue".parse::<OnPause>().unwrap(), OnPause::Requeue);
        assert_eq!(OnPause::default().to_string(), "finish");
        assert!("stop".parse::<OnPause>().is_err());
    }
}
