//! Network for microVMs whose job asks for it (`network = true`, or an
//! allow list since 1.18).
//!
//! The guest gets a virtio-net card whose other end is not a host network
//! device but this gateway: QEMU sends the card's Ethernet frames over a
//! local TCP connection (`-netdev socket`), and a user-space TCP/IP stack
//! (smoltcp) terminates the guest's connections. The vz backend
//! (Virtualization.framework) hands its card's frames to a datagram socket
//! instead (`VZFileHandleNetworkDeviceAttachment`), which the vz helper
//! joins to the same kind of TCP connection ([`bridge_datagrams`]), and
//! Firecracker's TAP device is joined to it the same way ([`tap`]), so
//! every backend meets the same gateway and the same policy. For each TCP
//! connection the guest opens, the gateway opens one from the host to the
//! same address, if that address is allowed, and copies bytes both ways.
//! UDP datagrams to allowed addresses go out through a host socket per
//! flow, and the answers come back to the guest (1.18: QUIC, NTP, DNS to
//! other servers). DNS queries to the gateway are answered with the host's
//! resolver.
//!
//! What the guest can reach is decided here, on the host, by destination
//! address: public unicast IPv4, and public IPv6 when the host has IPv6.
//! Private ranges (RFC 1918, CGNAT, ULA), loopback, link-local (cloud
//! metadata services), multicast, reserved ranges and every address of the
//! host itself are refused, as is SMTP (port 25). A job's allow list
//! ([`NetSpec`]) narrows that further: names it lists resolve, and their
//! addresses open for the ports it gives; addresses and ranges it lists
//! open directly. Its bandwidth cap paces what goes each way, and every
//! byte is counted ([`Gateway::usage`]). The guest cannot get around this
//! from inside: it only ever talks to the gateway.
//!
//! Guest side: address [`GUEST`]/24, gateway and DNS server [`GATEWAY`];
//! with IPv6, [`GUEST6`]/64 and gateway [`GATEWAY6`].

use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use pumasi_proto::vmnet::{NetSpec, NetUsage, Port};
use smoltcp::{
    iface::{Config, Interface, SocketHandle, SocketSet},
    phy::{ChecksumCapabilities, Device, DeviceCapabilities, Medium, RxToken, TxToken},
    socket::{tcp, udp},
    time::Instant as SmolInstant,
    wire::{
        EthernetAddress, EthernetFrame, EthernetProtocol, EthernetRepr, HardwareAddress, IpAddress,
        IpCidr, IpEndpoint, IpListenEndpoint, IpProtocol, IpRepr, Ipv4Packet, Ipv6Packet,
        TcpPacket, UdpPacket, UdpRepr,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};
use tracing::{debug, trace};

/// The gateway's address on the guest's network (also its DNS server).
pub const GATEWAY: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 2);
/// The guest's address.
pub const GUEST: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 15);
pub const PREFIX: u8 = 24;
/// The gateway's IPv6 address (a unique local one: the guest's traffic
/// leaves from the host's own addresses).
pub const GATEWAY6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x5055, 0x4d41, 0, 0, 0, 0, 2);
/// The guest's IPv6 address.
pub const GUEST6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x5055, 0x4d41, 0, 0, 0, 0, 0x15);
pub const PREFIX6: u8 = 64;
/// The guest card's MAC address (QEMU's default range).
pub const GUEST_MAC: &str = "52:54:00:12:34:56";
const GATEWAY_MAC: [u8; 6] = [0x52, 0x54, 0x00, 0x12, 0x34, 0x02];

/// Ethernet frame payload size (the guest's MTU).
const MTU: usize = 1500;
/// Largest frame accepted from QEMU (with the Ethernet header and room
/// for a VLAN tag).
pub(crate) const MAX_FRAME: usize = 65_536;
/// TCP buffers per connection on the guest side.
const TCP_BUFFER: usize = 256 * 1024;
/// Bytes read from a host connection but not yet taken by the guest.
const HOST_WINDOW: usize = 256 * 1024;
const CHUNK: usize = 64 * 1024;
const MAX_CONNECTIONS: usize = 512;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Tests: where a connection to a destination really goes. A guest can
/// only reach the gateway's host through it with an address that is not
/// loopback (smoltcp keeps `::1` to itself), and CI runners may have no
/// IPv6 at all, so the IPv6 test points a documentation address at a
/// local IPv4 listener.
#[cfg(test)]
static REDIRECT: std::sync::LazyLock<Mutex<HashMap<SocketAddr, SocketAddr>>> =
    std::sync::LazyLock::new(Default::default);
/// A connection the guest started but never completed.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long QEMU (or the vz helper) has to connect after the gateway
/// starts.
const ATTACH_TIMEOUT: Duration = Duration::from_secs(120);
const DNS_PORT: u16 = 53;
const MAX_DNS_IN_FLIGHT: usize = 64;
/// UDP flows (guest address and port, destination) open at once.
const MAX_UDP_FLOWS: usize = 256;
/// A UDP flow nothing crossed for this long is closed.
const UDP_IDLE: Duration = Duration::from_secs(120);
/// Addresses an allow list's names opened (what DNS answered).
const MAX_LEARNED: usize = 4096;
/// Ports never forwarded (outgoing mail would be spam from the provider's
/// address).
const BLOCKED_PORTS: &[u16] = &[25];

pub use pumasi_proto::vmnet::{public_ip, public_v4, public_v6};

/// Whether `ip` is one of this machine's own addresses: only those can be
/// bound.
fn host_address(ip: IpAddr) -> bool {
    std::net::UdpSocket::bind((ip, 0)).is_ok()
}

/// Whether this machine reaches the IPv6 internet: it has a route to a
/// public IPv6 address (nothing is sent).
pub fn host_has_ipv6() -> bool {
    std::net::UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0))
        .and_then(|s| {
            s.connect((
                Ipv6Addr::new(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888),
                53,
            ))?;
            s.local_addr()
        })
        .is_ok_and(|a| matches!(a.ip(), IpAddr::V6(v6) if public_v6(v6)))
}

/// Decides which destinations the guest may reach at all; a job's allow
/// list narrows it.
pub type Policy = Arc<dyn Fn(IpAddr, u16) -> bool + Send + Sync>;

/// The default policy: public addresses that are not this host's, and
/// not the blocked ports.
pub fn default_policy() -> Policy {
    let seen = Mutex::new(HashMap::<IpAddr, bool>::new());
    Arc::new(move |ip, port| {
        if BLOCKED_PORTS.contains(&port) || !public_ip(ip) {
            return false;
        }
        let mut seen = seen.lock().unwrap_or_else(|e| e.into_inner());
        *seen.entry(ip).or_insert_with(|| !host_address(ip))
    })
}

/// The policy and a job's allow list together.
struct Guard {
    base: Policy,
    spec: NetSpec,
    /// Addresses DNS answered for names the list allows, and the ports
    /// they open (`None`: every port).
    learned: Mutex<HashMap<IpAddr, Option<Vec<Port>>>>,
}

impl Guard {
    fn allows(&self, ip: IpAddr, port: u16) -> bool {
        if !(self.base)(ip, port) {
            return false;
        }
        if self.spec.allow.is_empty() || self.spec.allows_addr(ip, port) {
            return true;
        }
        match self.learned.lock().unwrap().get(&ip) {
            Some(None) => true,
            Some(Some(ports)) => ports.iter().any(|p| p.contains(port)),
            None => false,
        }
    }

    /// Opens what DNS answered for `name`, as far as the list allows it.
    fn learn(&self, name: &str, ips: &[IpAddr]) {
        if self.spec.allow.is_empty() {
            return;
        }
        let Some(ports) = self.spec.ports_for_name(name) else {
            return;
        };
        let mut learned = self.learned.lock().unwrap();
        for ip in ips {
            if learned.len() >= MAX_LEARNED && !learned.contains_key(ip) {
                break;
            }
            let e = learned.entry(*ip).or_insert_with(|| Some(Vec::new()));
            match (&mut *e, &ports) {
                (None, _) => {}
                (Some(_), None) => *e = None,
                (Some(have), Some(more)) => {
                    for p in more {
                        if !have.contains(p) {
                            have.push(*p);
                        }
                    }
                }
            }
        }
    }
}

/// Paces bytes to a rate: a token bucket that may go into debt, whose
/// takers wait the debt off.
struct Bucket {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, std::time::Instant)>,
}

impl Bucket {
    fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        let burst = (rate / 4.0).max(32.0 * 1024.0);
        Self {
            rate,
            burst,
            state: Mutex::new((burst, std::time::Instant::now())),
        }
    }

    fn refill(&self, s: &mut (f64, std::time::Instant)) {
        let now = std::time::Instant::now();
        s.0 = (s.0 + now.duration_since(s.1).as_secs_f64() * self.rate).min(self.burst);
        s.1 = now;
    }

    /// Takes `n` bytes, and says how long to wait before sending more.
    fn take(&self, n: usize) -> Duration {
        let mut s = self.state.lock().unwrap();
        self.refill(&mut s);
        s.0 -= n as f64;
        if s.0 >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-s.0 / self.rate)
        }
    }

    /// Takes `n` bytes if they are there (datagrams over the cap are
    /// dropped, as a full link would).
    fn try_take(&self, n: usize) -> bool {
        let mut s = self.state.lock().unwrap();
        self.refill(&mut s);
        if s.0 >= n as f64 {
            s.0 -= n as f64;
            true
        } else {
            false
        }
    }
}

/// Counts and paces a guest's traffic.
#[derive(Default)]
struct Meter {
    tx: AtomicU64,
    rx: AtomicU64,
    up: Option<Bucket>,
    down: Option<Bucket>,
}

impl Meter {
    fn new(spec: &NetSpec) -> Self {
        let rate = spec.bytes_per_sec();
        Self {
            up: rate.map(Bucket::new),
            down: rate.map(Bucket::new),
            ..Default::default()
        }
    }

    async fn sent(&self, n: usize) {
        if let Some(b) = &self.up {
            let wait = b.take(n);
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }
        self.tx.fetch_add(n as u64, Ordering::Relaxed);
    }

    async fn received(&self, n: usize) {
        if let Some(b) = &self.down {
            let wait = b.take(n);
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }
        self.rx.fetch_add(n as u64, Ordering::Relaxed);
    }

    fn try_send(&self, n: usize) -> bool {
        if self.up.as_ref().is_some_and(|b| !b.try_take(n)) {
            return false;
        }
        self.tx.fetch_add(n as u64, Ordering::Relaxed);
        true
    }

    fn try_receive(&self, n: usize) -> bool {
        if self.down.as_ref().is_some_and(|b| !b.try_take(n)) {
            return false;
        }
        self.rx.fetch_add(n as u64, Ordering::Relaxed);
        true
    }

    fn usage(&self) -> NetUsage {
        NetUsage {
            tx_bytes: self.tx.load(Ordering::Relaxed),
            rx_bytes: self.rx.load(Ordering::Relaxed),
        }
    }
}

/// How a gateway is set up beyond its policy.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The job's allow list and cap.
    pub spec: NetSpec,
    /// Give the guest IPv6 (the host has it).
    pub ipv6: bool,
}

/// A running gateway for one VM. Dropping it ends every connection.
pub struct Gateway {
    /// Where QEMU connects (`-netdev socket,connect=...`), or the vz
    /// helper's [`bridge_datagrams`].
    pub addr: SocketAddr,
    /// The guest gets IPv6.
    pub ipv6: bool,
    meter: Arc<Meter>,
    task: JoinHandle<()>,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Gateway {
    /// A gateway with the default policy, the job's allow list and cap,
    /// and IPv6 when the host has it.
    pub async fn start(spec: NetSpec) -> Result<Self> {
        let ipv6 = tokio::task::spawn_blocking(host_has_ipv6)
            .await
            .unwrap_or(false);
        Self::start_with(default_policy(), Options { spec, ipv6 }).await
    }

    pub async fn start_with(policy: Policy, opts: Options) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("listen for the VM's network card")?;
        let addr = listener.local_addr()?;
        let meter = Arc::new(Meter::new(&opts.spec));
        let guard = Arc::new(Guard {
            base: policy,
            spec: opts.spec,
            learned: Default::default(),
        });
        let ipv6 = opts.ipv6;
        let m = meter.clone();
        let task = tokio::spawn(async move {
            let stream = match tokio::time::timeout(ATTACH_TIMEOUT, listener.accept()).await {
                Ok(Ok((s, peer))) if peer.ip().is_loopback() => s,
                Ok(Ok((_, peer))) => {
                    debug!(%peer, "refused a network card connection");
                    return;
                }
                _ => return,
            };
            drop(listener);
            let _ = stream.set_nodelay(true);
            if let Err(e) = serve(stream, guard, m, ipv6).await {
                debug!("VM network ended: {e:#}");
            }
        });
        Ok(Self {
            addr,
            ipv6,
            meter,
            task,
        })
    }

    /// What the guest's network carried so far.
    pub fn usage(&self) -> NetUsage {
        self.meter.usage()
    }

    /// The settings the guest init brings its card up with.
    pub fn guest_net(&self) -> pumasi_guest::GuestNet {
        pumasi_guest::GuestNet {
            address: GUEST.octets(),
            prefix: PREFIX,
            gateway: GATEWAY.octets(),
            dns: GATEWAY.octets(),
            v6: self.ipv6.then(|| pumasi_guest::GuestNet6 {
                address: GUEST6.octets(),
                prefix: PREFIX6,
                gateway: GATEWAY6.octets(),
            }),
        }
    }
}

/// Frames in and out of smoltcp.
#[derive(Default)]
struct Queues {
    rx: VecDeque<Vec<u8>>,
    tx: VecDeque<Vec<u8>>,
}

struct Rx(Vec<u8>);
struct Tx<'a>(&'a mut VecDeque<Vec<u8>>);

impl RxToken for Rx {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

impl TxToken for Tx<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut buf = vec![0; len];
        let r = f(&mut buf);
        self.0.push_back(buf);
        r
    }
}

impl Device for Queues {
    type RxToken<'a> = Rx;
    type TxToken<'a> = Tx<'a>;

    fn receive(&mut self, _: SmolInstant) -> Option<(Rx, Tx<'_>)> {
        let f = self.rx.pop_front()?;
        Some((Rx(f), Tx(&mut self.tx)))
    }

    fn transmit(&mut self, _: SmolInstant) -> Option<Tx<'_>> {
        Some(Tx(&mut self.tx))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut c = DeviceCapabilities::default();
        c.medium = Medium::Ethernet;
        c.max_transmission_unit = MTU + 14;
        c
    }
}

/// What connection tasks tell the main loop.
enum Event {
    /// Bytes from the host for the guest.
    Data(u64, Vec<u8>),
    /// The host closed its side.
    Eof(u64),
    /// The host connection failed or broke.
    Failed(u64),
    /// A DNS answer for the guest.
    Dns(IpEndpoint, Vec<u8>),
    /// A datagram from the host side of a UDP flow.
    Udp(u64, Vec<u8>),
}

/// One guest TCP connection.
struct Conn {
    handle: SocketHandle,
    key: FlowKey,
    created: std::time::Instant,
    /// Guest-to-host bytes; dropped when the guest closes its side.
    to_host: Option<mpsc::Sender<Vec<u8>>>,
    /// Host-to-guest bytes not yet in the socket's buffer.
    pending: VecDeque<Vec<u8>>,
    offset: usize,
    credit: Arc<Semaphore>,
    host_eof: bool,
    fin_sent: bool,
    opened: bool,
    task: JoinHandle<()>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Guest source port, destination address and port.
type FlowKey = (u16, IpAddr, u16);

/// One UDP flow: a host socket connected to the destination.
struct UdpFlow {
    key: (SocketAddr, SocketAddr),
    socket: Arc<tokio::net::UdpSocket>,
    last: std::time::Instant,
    task: JoinHandle<()>,
}

impl Drop for UdpFlow {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The IP layer of a frame from the guest: version, source, destination,
/// protocol and payload. Fragments are not handled (the guest's MTU fits
/// what it sends).
fn ip_of(frame: &[u8]) -> Option<(IpAddr, IpAddr, IpProtocol, &[u8])> {
    let eth = EthernetFrame::new_checked(frame).ok()?;
    let at = EthernetFrame::<&[u8]>::header_len();
    match eth.ethertype() {
        EthernetProtocol::Ipv4 => {
            let ip = Ipv4Packet::new_checked(&frame[at..]).ok()?;
            if ip.more_frags() || ip.frag_offset() != 0 {
                return None;
            }
            let hl = ip.header_len() as usize;
            let total = (ip.total_len() as usize).min(frame.len() - at);
            Some((
                IpAddr::V4(ip.src_addr()),
                IpAddr::V4(ip.dst_addr()),
                ip.next_header(),
                frame.get(at + hl..at + total)?,
            ))
        }
        EthernetProtocol::Ipv6 => {
            let ip = Ipv6Packet::new_checked(&frame[at..]).ok()?;
            let len = (ip.payload_len() as usize).min(frame.len() - at - 40);
            Some((
                IpAddr::V6(ip.src_addr()),
                IpAddr::V6(ip.dst_addr()),
                ip.next_header(),
                frame.get(at + 40..at + 40 + len)?,
            ))
        }
        _ => None,
    }
}

/// A TCP SYN from the guest: its flow.
fn syn_flow(frame: &[u8]) -> Option<FlowKey> {
    let (_, dst, proto, payload) = ip_of(frame)?;
    if proto != IpProtocol::Tcp {
        return None;
    }
    let tcp = TcpPacket::new_checked(payload).ok()?;
    if !tcp.syn() || tcp.ack() {
        return None;
    }
    Some((tcp.src_port(), dst, tcp.dst_port()))
}

/// A UDP datagram from the guest: its source, destination and payload.
fn guest_udp(frame: &[u8]) -> Option<(SocketAddr, SocketAddr, &[u8])> {
    let (src, dst, proto, payload) = ip_of(frame)?;
    if proto != IpProtocol::Udp {
        return None;
    }
    let udp = UdpPacket::new_checked(payload).ok()?;
    let len = (udp.len() as usize).clamp(8, payload.len());
    Some((
        SocketAddr::new(src, udp.src_port()),
        SocketAddr::new(dst, udp.dst_port()),
        &payload[8..len],
    ))
}

/// An Ethernet frame carrying a datagram from `from` to the guest at `to`.
fn udp_frame(
    guest_mac: EthernetAddress,
    from: SocketAddr,
    to: SocketAddr,
    payload: &[u8],
) -> Option<Vec<u8>> {
    let (src, dst, ethertype) = match (from.ip(), to.ip()) {
        (IpAddr::V4(a), IpAddr::V4(b)) => (
            IpAddress::Ipv4(a),
            IpAddress::Ipv4(b),
            EthernetProtocol::Ipv4,
        ),
        (IpAddr::V6(a), IpAddr::V6(b)) => (
            IpAddress::Ipv6(a),
            IpAddress::Ipv6(b),
            EthernetProtocol::Ipv6,
        ),
        _ => return None,
    };
    let udp = UdpRepr {
        src_port: from.port(),
        dst_port: to.port(),
    };
    let ip = IpRepr::new(
        src,
        dst,
        IpProtocol::Udp,
        udp.header_len() + payload.len(),
        64,
    );
    let eth = EthernetRepr {
        src_addr: EthernetAddress(GATEWAY_MAC),
        dst_addr: guest_mac,
        ethertype,
    };
    let ip_len = ip.header_len() + udp.header_len() + payload.len();
    if ip_len > MTU {
        return None;
    }
    let mut buf = vec![0u8; eth.buffer_len() + ip_len];
    eth.emit(&mut EthernetFrame::new_unchecked(&mut buf[..]));
    let at = eth.buffer_len();
    let caps = ChecksumCapabilities::default();
    ip.emit(&mut buf[at..], &caps);
    let at = at + ip.header_len();
    udp.emit(
        &mut UdpPacket::new_unchecked(&mut buf[at..]),
        &src,
        &dst,
        payload.len(),
        |b| b.copy_from_slice(payload),
        &caps,
    );
    Some(buf)
}

fn is_gateway(ip: IpAddr) -> bool {
    ip == IpAddr::V4(GATEWAY) || ip == IpAddr::V6(GATEWAY6)
}

async fn serve(stream: TcpStream, guard: Arc<Guard>, meter: Arc<Meter>, ipv6: bool) -> Result<()> {
    let (mut rd, mut wr) = stream.into_split();
    // QEMU's socket netdev: each frame is a 4-byte big-endian length and
    // the frame.
    let (in_tx, mut in_rx) = mpsc::channel::<Vec<u8>>(256);
    let reader = tokio::spawn(async move {
        loop {
            let mut len = [0u8; 4];
            if rd.read_exact(&mut len).await.is_err() {
                return;
            }
            let len = u32::from_be_bytes(len) as usize;
            if len > MAX_FRAME {
                debug!(len, "oversized frame from the VM");
                return;
            }
            let mut f = vec![0; len];
            if rd.read_exact(&mut f).await.is_err() || in_tx.send(f).await.is_err() {
                return;
            }
        }
    });
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(1024);
    let writer = tokio::spawn(async move {
        while let Some(f) = out_rx.recv().await {
            let mut buf = Vec::with_capacity(4 + f.len());
            buf.extend_from_slice(&(f.len() as u32).to_be_bytes());
            buf.extend_from_slice(&f);
            if wr.write_all(&buf).await.is_err() {
                return;
            }
        }
    });
    let _guard = (AbortOnDrop(reader), AbortOnDrop(writer));

    let start = std::time::Instant::now();
    let now = || SmolInstant::from_micros(start.elapsed().as_micros() as i64);
    let mut dev = Queues::default();
    let mut config = Config::new(HardwareAddress::Ethernet(EthernetAddress(GATEWAY_MAC)));
    config.random_seed = rand_seed();
    let mut iface = Interface::new(config, &mut dev, now());
    iface.update_ip_addrs(|a| {
        let _ = a.push(IpCidr::new(IpAddress::Ipv4(GATEWAY), PREFIX));
        if ipv6 {
            let _ = a.push(IpCidr::new(IpAddress::Ipv6(GATEWAY6), PREFIX6));
        }
    });
    // Every destination routes through the gateway's own address, which
    // with any-IP makes smoltcp accept packets for any address.
    iface
        .routes_mut()
        .add_default_ipv4_route(GATEWAY)
        .map_err(|_| anyhow::anyhow!("no room for the default route"))?;
    if ipv6 {
        iface
            .routes_mut()
            .add_default_ipv6_route(GATEWAY6)
            .map_err(|_| anyhow::anyhow!("no room for the IPv6 default route"))?;
    }
    iface.set_any_ip(true);
    let mut sockets = SocketSet::new(Vec::new());
    let mut dns_socket = |addr: IpAddress| -> Result<SocketHandle> {
        let mut s = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 32], vec![0; 32 * 600]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 32], vec![0; 32 * 1500]),
        );
        s.bind(IpListenEndpoint {
            addr: Some(addr),
            port: DNS_PORT,
        })
        .map_err(|e| anyhow::anyhow!("bind DNS: {e}"))?;
        Ok(sockets.add(s))
    };
    let mut dns = vec![dns_socket(IpAddress::Ipv4(GATEWAY))?];
    if ipv6 {
        dns.push(dns_socket(IpAddress::Ipv6(GATEWAY6))?);
    }
    let dns_slots = Arc::new(Semaphore::new(MAX_DNS_IN_FLIGHT));
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Event>();
    let mut conns: HashMap<u64, Conn> = HashMap::new();
    let mut flows: HashMap<FlowKey, u64> = HashMap::new();
    let mut udp_flows: HashMap<u64, UdpFlow> = HashMap::new();
    let mut udp_keys: HashMap<(SocketAddr, SocketAddr), u64> = HashMap::new();
    let mut guest_mac: Option<EthernetAddress> = None;
    let mut last_sweep = std::time::Instant::now();
    let mut next_id = 0u64;
    let mut buf = vec![0u8; CHUNK];

    loop {
        let busy = conns.values().any(|c| {
            !c.pending.is_empty() || c.to_host.as_ref().is_some_and(|t| t.capacity() == 0)
        });
        let mut delay = iface
            .poll_delay(now(), &sockets)
            .map(|d| Duration::from_micros(d.total_micros()))
            .unwrap_or(Duration::from_secs(1));
        if busy {
            delay = delay.min(Duration::from_millis(5));
        }
        let mut frames = Vec::new();
        let mut events = Vec::new();
        tokio::select! {
            f = in_rx.recv() => match f {
                Some(f) => frames.push(f),
                None => bail!("the VM closed its network card"),
            },
            e = ev_rx.recv() => {
                if let Some(e) = e {
                    events.push(e);
                }
            }
            _ = tokio::time::sleep(delay) => {}
        }
        while frames.len() < 64 {
            match in_rx.try_recv() {
                Ok(f) => frames.push(f),
                Err(_) => break,
            }
        }
        while let Ok(e) = ev_rx.try_recv() {
            events.push(e);
        }

        for e in events {
            match e {
                Event::Data(id, data) => {
                    if let Some(c) = conns.get_mut(&id) {
                        c.pending.push_back(data);
                    }
                }
                Event::Eof(id) => {
                    if let Some(c) = conns.get_mut(&id) {
                        c.host_eof = true;
                    }
                }
                Event::Failed(id) => {
                    if let Some(c) = conns.get(&id) {
                        trace!(flow = ?c.key, "host connection failed");
                        sockets.get_mut::<tcp::Socket>(c.handle).abort();
                    }
                }
                Event::Dns(to, answer) => {
                    let h = match to.addr {
                        IpAddress::Ipv6(_) if dns.len() > 1 => dns[1],
                        _ => dns[0],
                    };
                    let _ = sockets.get_mut::<udp::Socket>(h).send_slice(&answer, to);
                }
                Event::Udp(id, data) => {
                    let (Some(flow), Some(mac)) = (udp_flows.get_mut(&id), guest_mac) else {
                        continue;
                    };
                    flow.last = std::time::Instant::now();
                    let (guest, dst) = flow.key;
                    if meter.try_receive(data.len())
                        && let Some(f) = udp_frame(mac, dst, guest, &data)
                    {
                        let _ = out_tx.try_send(f);
                    }
                }
            }
        }

        for f in frames {
            if let Ok(eth) = EthernetFrame::new_checked(&f[..]) {
                guest_mac.get_or_insert(eth.src_addr());
            }
            // UDP to anywhere but the gateway: out through a host socket
            // when allowed. Refused datagrams go on to smoltcp, which
            // answers them with "port unreachable".
            if let Some((src, dst, payload)) = guest_udp(&f)
                && !is_gateway(dst.ip())
                && guard.allows(dst.ip(), dst.port())
            {
                let key = (src, dst);
                let id = match udp_keys.get(&key) {
                    Some(id) => Some(*id),
                    None if udp_flows.len() < MAX_UDP_FLOWS => {
                        match open_udp(dst, next_id, ev_tx.clone()) {
                            Ok((socket, task)) => {
                                let id = next_id;
                                next_id += 1;
                                udp_keys.insert(key, id);
                                udp_flows.insert(
                                    id,
                                    UdpFlow {
                                        key,
                                        socket,
                                        last: std::time::Instant::now(),
                                        task,
                                    },
                                );
                                Some(id)
                            }
                            Err(e) => {
                                trace!(%dst, "UDP flow not opened: {e:#}");
                                None
                            }
                        }
                    }
                    None => None,
                };
                if let Some(flow) = id.and_then(|id| udp_flows.get_mut(&id)) {
                    flow.last = std::time::Instant::now();
                    if meter.try_send(payload.len()) {
                        let _ = flow.socket.try_send(payload);
                    }
                }
                continue;
            }
            if let Some(key) = syn_flow(&f)
                && !flows.contains_key(&key)
                && conns.len() < MAX_CONNECTIONS
                && guard.allows(key.1, key.2)
            {
                // A listener for exactly this destination, before smoltcp
                // sees the SYN. Refused destinations have none, and
                // smoltcp answers them with a reset.
                let mut s = tcp::Socket::new(
                    tcp::SocketBuffer::new(vec![0; TCP_BUFFER]),
                    tcp::SocketBuffer::new(vec![0; TCP_BUFFER]),
                );
                s.set_nagle_enabled(false);
                s.set_ack_delay(None);
                s.set_timeout(Some(smoltcp::time::Duration::from_secs(600)));
                s.set_keep_alive(Some(smoltcp::time::Duration::from_secs(60)));
                if s.listen(IpListenEndpoint {
                    addr: Some(key.1.into()),
                    port: key.2,
                })
                .is_ok()
                {
                    let handle = sockets.add(s);
                    let id = next_id;
                    next_id += 1;
                    let (to_host, from_guest) = mpsc::channel::<Vec<u8>>(16);
                    let credit = Arc::new(Semaphore::new(HOST_WINDOW));
                    let task = tokio::spawn(connect(
                        id,
                        SocketAddr::new(key.1, key.2),
                        from_guest,
                        credit.clone(),
                        ev_tx.clone(),
                        meter.clone(),
                    ));
                    flows.insert(key, id);
                    conns.insert(
                        id,
                        Conn {
                            handle,
                            key,
                            created: std::time::Instant::now(),
                            to_host: Some(to_host),
                            pending: VecDeque::new(),
                            offset: 0,
                            credit,
                            host_eof: false,
                            fin_sent: false,
                            opened: false,
                            task,
                        },
                    );
                }
            }
            dev.rx.push_back(f);
        }

        iface.poll(now(), &mut dev, &mut sockets);

        // DNS queries.
        for &h in &dns {
            loop {
                let s = sockets.get_mut::<udp::Socket>(h);
                let Ok((n, meta)) = s.recv_slice(&mut buf) else {
                    break;
                };
                let query = buf[..n].to_vec();
                let Ok(slot) = dns_slots.clone().try_acquire_owned() else {
                    continue;
                };
                let ev = ev_tx.clone();
                let guard = guard.clone();
                tokio::spawn(async move {
                    if let Some(answer) = resolve(&query, &guard, ipv6).await {
                        let _ = ev.send(Event::Dns(meta.endpoint, answer));
                    }
                    drop(slot);
                });
            }
        }

        // TCP connections.
        let mut done = Vec::new();
        for (id, c) in conns.iter_mut() {
            let s = sockets.get_mut::<tcp::Socket>(c.handle);
            let state = s.state();
            if !c.opened
                && !matches!(
                    state,
                    tcp::State::Listen | tcp::State::SynReceived | tcp::State::Closed
                )
            {
                c.opened = true;
            }
            // Guest to host, as fast as the host side takes it.
            while let Some(tx) = &c.to_host {
                if !s.can_recv() {
                    break;
                }
                let Ok(permit) = tx.try_reserve() else {
                    break;
                };
                match s.recv_slice(&mut buf) {
                    Ok(n) if n > 0 => permit.send(buf[..n].to_vec()),
                    _ => break,
                }
            }
            if c.opened && !s.may_recv() && s.recv_queue() == 0 {
                // The guest closed its side: pass that on.
                c.to_host = None;
            }
            // Host to guest, as fast as the guest takes it.
            while let Some(front) = c.pending.front() {
                if !s.can_send() {
                    break;
                }
                let n = s.send_slice(&front[c.offset..]).unwrap_or(0);
                if n == 0 {
                    break;
                }
                c.credit.add_permits(n);
                c.offset += n;
                if c.offset == front.len() {
                    c.pending.pop_front();
                    c.offset = 0;
                }
            }
            if c.host_eof && c.pending.is_empty() && !c.fin_sent && c.opened {
                s.close();
                c.fin_sent = true;
            }
            let stale = !c.opened && c.created.elapsed() > HANDSHAKE_TIMEOUT;
            let closed = c.opened && matches!(state, tcp::State::Closed | tcp::State::TimeWait);
            // Refused or broken before the handshake completed.
            let aborted = !c.opened && state == tcp::State::Closed;
            if stale || closed || aborted {
                done.push(*id);
            }
        }
        for id in done {
            if let Some(c) = conns.remove(&id) {
                flows.remove(&c.key);
                sockets.remove(c.handle);
            }
        }
        // UDP flows nothing crossed for a while.
        if last_sweep.elapsed() > Duration::from_secs(1) {
            last_sweep = std::time::Instant::now();
            udp_flows.retain(|_, f| {
                let keep = f.last.elapsed() < UDP_IDLE;
                if !keep {
                    udp_keys.remove(&f.key);
                }
                keep
            });
        }

        iface.poll(now(), &mut dev, &mut sockets);
        while let Some(f) = dev.tx.pop_front() {
            // A full queue drops the frame, as a busy link would; TCP
            // sends it again.
            let _ = out_tx.try_send(f);
        }
    }
}

/// A host socket for a guest's UDP flow to `dst`, and the task that
/// passes its answers on.
fn open_udp(
    dst: SocketAddr,
    id: u64,
    ev: mpsc::UnboundedSender<Event>,
) -> Result<(Arc<tokio::net::UdpSocket>, JoinHandle<()>)> {
    let any: SocketAddr = match dst {
        SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
    };
    let std = std::net::UdpSocket::bind(any).context("bind")?;
    std.connect(dst).context("connect")?;
    std.set_nonblocking(true)?;
    let socket = Arc::new(tokio::net::UdpSocket::from_std(std)?);
    let rd = socket.clone();
    let task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65_536];
        loop {
            match rd.recv(&mut buf).await {
                Ok(n) => {
                    if ev.send(Event::Udp(id, buf[..n].to_vec())).is_err() {
                        return;
                    }
                }
                // ICMP errors for earlier datagrams show up here; the flow
                // goes on.
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {}
                Err(_) => return,
            }
        }
    });
    Ok((socket, task))
}

/// Send and receive buffers of the datagram socket a vz VM's network card
/// uses. Apple asks for a receive buffer at least twice the send buffer.
#[cfg(unix)]
const DGRAM_SNDBUF: libc::c_int = 1 << 20;
#[cfg(unix)]
const DGRAM_RCVBUF: libc::c_int = 4 << 20;

/// Makes the pair of datagram sockets a vz network card needs: one end
/// for `VZFileHandleNetworkDeviceAttachment`, one for [`bridge_datagrams`].
#[cfg(unix)]
pub fn datagram_pair() -> Result<(
    std::os::unix::net::UnixDatagram,
    std::os::unix::net::UnixDatagram,
)> {
    use std::os::fd::AsRawFd;
    let (a, b) = std::os::unix::net::UnixDatagram::pair().context("make a datagram socket pair")?;
    for s in [&a, &b] {
        for (opt, size) in [
            (libc::SO_SNDBUF, DGRAM_SNDBUF),
            (libc::SO_RCVBUF, DGRAM_RCVBUF),
        ] {
            // SAFETY: an open socket and an int-sized option value.
            let r = unsafe {
                libc::setsockopt(
                    s.as_raw_fd(),
                    libc::SOL_SOCKET,
                    opt,
                    (&size as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if r != 0 {
                // Smaller buffers only drop more frames under load.
                debug!(
                    "setsockopt on the VM's datagram socket: {}",
                    std::io::Error::last_os_error()
                );
            }
        }
    }
    Ok((a, b))
}

/// Joins a datagram socket that carries one Ethernet frame per datagram
/// (the other end of a vz VM's `VZFileHandleNetworkDeviceAttachment`) to
/// a [`Gateway`], which speaks QEMU's stream framing (4-byte big-endian
/// length, then the frame). Runs on two plain threads, so it works in the
/// vz helper, which has no async runtime, and returns once both run. The
/// threads end when either side closes. The gateway filters everything,
/// exactly as for QEMU: this only changes how frames travel.
#[cfg(unix)]
pub fn bridge_datagrams(
    dgram: std::os::unix::net::UnixDatagram,
    gateway: SocketAddr,
) -> Result<[std::thread::JoinHandle<()>; 2]> {
    use std::io::{Read, Write};
    let stream = std::net::TcpStream::connect_timeout(&gateway, Duration::from_secs(10))
        .with_context(|| format!("connect to the VM network gateway at {gateway}"))?;
    let _ = stream.set_nodelay(true);
    let mut from_gw = stream.try_clone()?;
    let mut to_gw = stream;
    let to_vm = dgram.try_clone()?;
    let from_vm = dgram;

    let up = std::thread::Builder::new()
        .name("vz-net-up".into())
        .spawn(move || {
            let mut buf = vec![0u8; MAX_FRAME];
            let mut out = Vec::with_capacity(4 + MAX_FRAME);
            loop {
                let n = match from_vm.recv(&mut buf) {
                    // No frame is empty: the socket was shut down.
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                };
                out.clear();
                out.extend_from_slice(&(n as u32).to_be_bytes());
                out.extend_from_slice(&buf[..n]);
                if to_gw.write_all(&out).is_err() {
                    break;
                }
            }
            let _ = to_gw.shutdown(std::net::Shutdown::Both);
        })?;
    let down = std::thread::Builder::new()
        .name("vz-net-down".into())
        .spawn(move || {
            let mut frame = vec![0u8; MAX_FRAME];
            loop {
                let mut len = [0u8; 4];
                if from_gw.read_exact(&mut len).is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len) as usize;
                if len > MAX_FRAME || from_gw.read_exact(&mut frame[..len]).is_err() {
                    break;
                }
                send_frame(&to_vm, &frame[..len]);
            }
            let _ = to_vm.shutdown(std::net::Shutdown::Both);
        })?;
    Ok([up, down])
}

/// Hands one frame to the VM. A full socket buffer (macOS answers ENOBUFS
/// rather than blocking) is retried briefly, then the frame is dropped,
/// as a busy link would; TCP sends it again.
#[cfg(unix)]
fn send_frame(s: &std::os::unix::net::UnixDatagram, f: &[u8]) {
    for attempt in 0..50u32 {
        match s.send(f) {
            Ok(_) => return,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e)
                if e.raw_os_error() == Some(libc::ENOBUFS)
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                std::thread::sleep(Duration::from_micros(100 << attempt.min(4)));
            }
            Err(e) => {
                trace!("frame to the VM dropped: {e}");
                return;
            }
        }
    }
    trace!("frame to the VM dropped: its socket stayed full");
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The host side of one guest connection.
async fn connect(
    id: u64,
    to: SocketAddr,
    mut from_guest: mpsc::Receiver<Vec<u8>>,
    credit: Arc<Semaphore>,
    ev: mpsc::UnboundedSender<Event>,
    meter: Arc<Meter>,
) {
    #[cfg(test)]
    let to = REDIRECT.lock().unwrap().get(&to).copied().unwrap_or(to);
    let stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(to)).await {
        Ok(Ok(s)) => s,
        _ => {
            let _ = ev.send(Event::Failed(id));
            return;
        }
    };
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let m = meter.clone();
    let up = async move {
        while let Some(data) = from_guest.recv().await {
            m.sent(data.len()).await;
            wr.write_all(&data).await?;
        }
        wr.shutdown().await
    };
    let ev2 = ev.clone();
    let down = async move {
        let mut buf = vec![0u8; CHUNK];
        loop {
            let Ok(permit) = credit.acquire_many(CHUNK as u32).await else {
                return Ok::<_, std::io::Error>(());
            };
            permit.forget();
            let n = match rd.read(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    credit.add_permits(CHUNK);
                    return Err(e);
                }
            };
            credit.add_permits(CHUNK - n);
            if n == 0 {
                let _ = ev2.send(Event::Eof(id));
                return Ok(());
            }
            // Under a cap, reading waits: the sender's window fills and it
            // slows down.
            meter.received(n).await;
            if ev2.send(Event::Data(id, buf[..n].to_vec())).is_err() {
                return Ok(());
            }
        }
    };
    let (a, b) = tokio::join!(up, down);
    if a.is_err() || b.is_err() {
        let _ = ev.send(Event::Failed(id));
    }
}

/// Answers a DNS query with the host's resolver: A records, and AAAA
/// records when the guest has IPv6, only with addresses the guest may
/// reach. A name the job's allow list does not have does not exist; the
/// addresses of one it has open for the ports the list gives.
async fn resolve(query: &[u8], guard: &Guard, ipv6: bool) -> Option<Vec<u8>> {
    let q = parse_query(query)?;
    let want_v6 = match (q.qtype, q.qclass) {
        (1, 1) => false,
        (28, 1) => true,
        _ => return Some(answer(query, &q, Some(&[]))),
    };
    if !guard.spec.allows_name(&q.name) {
        return Some(answer(query, &q, None));
    }
    if want_v6 && !ipv6 {
        return Some(answer(query, &q, Some(&[])));
    }
    let addrs = match tokio::time::timeout(
        Duration::from_secs(10),
        tokio::net::lookup_host((q.name.as_str(), 0)),
    )
    .await
    {
        Ok(Ok(it)) => {
            let ips: Vec<IpAddr> = it
                .map(|a| a.ip())
                .filter(|ip| ip.is_ipv6() == want_v6)
                .filter(|ip| (guard.base)(*ip, 443) || (guard.base)(*ip, 80))
                .take(16)
                .collect();
            guard.learn(&q.name, &ips);
            Some(ips)
        }
        _ => None,
    };
    Some(answer(query, &q, addrs.as_deref()))
}

#[derive(Debug, PartialEq)]
struct Query {
    name: String,
    qtype: u16,
    qclass: u16,
    /// Where the question ends in the packet.
    end: usize,
}

/// The single question of a DNS query.
fn parse_query(p: &[u8]) -> Option<Query> {
    if p.len() < 12 || p[2] & 0x80 != 0 || u16::from_be_bytes([p[4], p[5]]) != 1 {
        return None;
    }
    let mut i = 12;
    let mut labels = Vec::new();
    loop {
        let len = *p.get(i)? as usize;
        i += 1;
        if len == 0 {
            break;
        }
        // No compression in a question; 63 is the longest label.
        if len > 63 {
            return None;
        }
        let label = p.get(i..i + len)?;
        if !label
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
        {
            return None;
        }
        labels.push(String::from_utf8_lossy(label).into_owned());
        i += len;
    }
    let name = labels.join(".");
    if name.is_empty() || name.len() > 253 {
        return None;
    }
    let t = p.get(i..i + 4)?;
    Some(Query {
        name,
        qtype: u16::from_be_bytes([t[0], t[1]]),
        qclass: u16::from_be_bytes([t[2], t[3]]),
        end: i + 4,
    })
}

/// The response to `query`: these A or AAAA records, or NXDOMAIN for
/// `None`.
fn answer(query: &[u8], q: &Query, addrs: Option<&[IpAddr]>) -> Vec<u8> {
    let mut r = Vec::with_capacity(q.end + 16 * 28);
    r.extend_from_slice(&query[..2]);
    // Response, recursion desired (as asked) and available.
    let rd = query[2] & 0x01;
    r.push(0x80 | rd);
    r.push(if addrs.is_some() { 0x80 } else { 0x83 });
    let n = addrs.map_or(0, |a| a.len()) as u16;
    r.extend_from_slice(&1u16.to_be_bytes());
    r.extend_from_slice(&n.to_be_bytes());
    r.extend_from_slice(&[0, 0, 0, 0]);
    r.extend_from_slice(&query[12..q.end]);
    for a in addrs.unwrap_or_default() {
        // The name is the question's (a pointer to offset 12), class IN,
        // a minute to live.
        match a {
            IpAddr::V4(v4) => {
                r.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4]);
                r.extend_from_slice(&v4.octets());
            }
            IpAddr::V6(v6) => {
                r.extend_from_slice(&[0xc0, 0x0c, 0, 28, 0, 1, 0, 0, 0, 60, 0, 16]);
                r.extend_from_slice(&v6.octets());
            }
        }
    }
    r
}

fn rand_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(std::process::id() as u64);
    h.finish()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddrV4;

    use super::*;

    #[test]
    fn only_public_addresses() {
        for ip in [
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "127.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "0.1.2.3",
            "224.0.0.1",
            "255.255.255.255",
            "240.0.0.1",
            "192.0.0.8",
            "198.18.0.1",
            "192.0.2.1",
        ] {
            assert!(!public_v4(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "100.128.0.1",
            "172.32.0.1",
            "93.184.216.34",
        ] {
            assert!(public_v4(ip.parse().unwrap()), "{ip}");
        }
        let p = default_policy();
        assert!(p("1.1.1.1".parse().unwrap(), 443));
        assert!(!p("1.1.1.1".parse().unwrap(), 25), "no mail");
        assert!(!p("10.0.2.2".parse().unwrap(), 80));
    }

    fn query(name: &str, qtype: u16) -> Vec<u8> {
        let mut q = vec![0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
        for l in name.split('.') {
            q.push(l.len() as u8);
            q.extend_from_slice(l.as_bytes());
        }
        q.push(0);
        q.extend_from_slice(&qtype.to_be_bytes());
        q.extend_from_slice(&1u16.to_be_bytes());
        q
    }

    #[test]
    fn dns_answers() {
        let q = query("pypi.org", 1);
        let parsed = parse_query(&q).unwrap();
        assert_eq!(parsed.name, "pypi.org");
        assert_eq!(parsed.qtype, 1);
        assert_eq!(parsed.end, q.len());
        let ip: IpAddr = "151.101.0.223".parse().unwrap();
        let r = answer(&q, &parsed, Some(&[ip]));
        assert_eq!(&r[..2], &[0x12, 0x34]);
        assert_eq!(r[2] & 0x80, 0x80, "a response");
        assert_eq!(r[3] & 0x0f, 0, "no error");
        assert_eq!(u16::from_be_bytes([r[6], r[7]]), 1, "one answer");
        assert_eq!(&r[r.len() - 4..], &[151, 101, 0, 223]);
        // AAAA records carry 16 bytes.
        let q6 = query("example.com", 28);
        let p6 = parse_query(&q6).unwrap();
        let v6: IpAddr = "2606:2800:220:1::1".parse().unwrap();
        let r6 = answer(&q6, &p6, Some(&[v6]));
        assert_eq!(&r6[r6.len() - 16..], &v6_octets(v6));
        assert_eq!(&r6[r6.len() - 26..r6.len() - 24], &[0, 28], "type AAAA");
        let nx = answer(&q, &parsed, None);
        assert_eq!(nx[3] & 0x0f, 3, "NXDOMAIN");
        assert_eq!(u16::from_be_bytes([nx[6], nx[7]]), 0);
        // Responses, compressed names and junk are not queries.
        let mut resp = q.clone();
        resp[2] |= 0x80;
        assert!(parse_query(&resp).is_none());
        assert!(parse_query(&q[..14]).is_none());
        let mut bad = q.clone();
        bad[12] = 0xc0;
        assert!(parse_query(&bad).is_none());
    }

    /// A guest made of a second smoltcp stack, talking to the gateway the
    /// way QEMU does.
    struct FakeGuest {
        link: Link,
        dev: Queues,
        iface: Interface,
        sockets: SocketSet<'static>,
        start: std::time::Instant,
        inbuf: Vec<u8>,
    }

    /// How the fake guest's frames reach the gateway.
    enum Link {
        /// QEMU's `-netdev socket`.
        Stream(TcpStream),
        /// A vz card's datagram socket, through [`bridge_datagrams`].
        #[cfg(unix)]
        Datagram(tokio::net::UnixDatagram),
    }

    impl FakeGuest {
        async fn attach(gw: &Gateway) -> Self {
            let stream = TcpStream::connect(gw.addr).await.unwrap();
            Self::with_link(Link::Stream(stream))
        }

        /// Attached the way the vz helper attaches a VM.
        #[cfg(unix)]
        async fn attach_datagram(gw: &Gateway) -> Self {
            let (vm, ours) = datagram_pair().unwrap();
            let addr = gw.addr;
            tokio::task::spawn_blocking(move || bridge_datagrams(ours, addr).unwrap())
                .await
                .unwrap();
            vm.set_nonblocking(true).unwrap();
            Self::with_link(Link::Datagram(
                tokio::net::UnixDatagram::from_std(vm).unwrap(),
            ))
        }

        fn with_link(link: Link) -> Self {
            let mut dev = Queues::default();
            let start = std::time::Instant::now();
            let mac = EthernetAddress([0x52, 0x54, 0x00, 0x12, 0x34, 0x56]);
            let mut iface = Interface::new(
                Config::new(HardwareAddress::Ethernet(mac)),
                &mut dev,
                SmolInstant::from_millis(0),
            );
            iface.update_ip_addrs(|a| {
                let _ = a.push(IpCidr::new(IpAddress::Ipv4(GUEST), PREFIX));
                let _ = a.push(IpCidr::new(IpAddress::Ipv6(GUEST6), PREFIX6));
            });
            iface.routes_mut().add_default_ipv4_route(GATEWAY).unwrap();
            iface.routes_mut().add_default_ipv6_route(GATEWAY6).unwrap();
            Self {
                link,
                dev,
                iface,
                sockets: SocketSet::new(Vec::new()),
                start,
                inbuf: Vec::new(),
            }
        }

        fn now(&self) -> SmolInstant {
            SmolInstant::from_micros(self.start.elapsed().as_micros() as i64)
        }

        /// Moves frames both ways for a while.
        async fn pump(&mut self, ms: u64) {
            let until = std::time::Instant::now() + Duration::from_millis(ms);
            while std::time::Instant::now() < until {
                let now = self.now();
                self.iface.poll(now, &mut self.dev, &mut self.sockets);
                let stream = match &mut self.link {
                    Link::Stream(s) => s,
                    #[cfg(unix)]
                    Link::Datagram(d) => {
                        while let Some(f) = self.dev.tx.pop_front() {
                            d.send(&f).await.unwrap();
                        }
                        let mut tmp = vec![0u8; 65536];
                        while let Ok(Ok(n)) =
                            tokio::time::timeout(Duration::from_millis(2), d.recv(&mut tmp)).await
                        {
                            self.dev.rx.push_back(tmp[..n].to_vec());
                        }
                        continue;
                    }
                };
                while let Some(f) = self.dev.tx.pop_front() {
                    let mut b = (f.len() as u32).to_be_bytes().to_vec();
                    b.extend_from_slice(&f);
                    stream.write_all(&b).await.unwrap();
                }
                let mut tmp = [0u8; 65536];
                if let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_millis(2), stream.read(&mut tmp)).await
                {
                    self.inbuf.extend_from_slice(&tmp[..n]);
                }
                while self.inbuf.len() >= 4 {
                    let len = u32::from_be_bytes(self.inbuf[..4].try_into().unwrap()) as usize;
                    if self.inbuf.len() < 4 + len {
                        break;
                    }
                    let f = self.inbuf[4..4 + len].to_vec();
                    self.inbuf.drain(..4 + len);
                    self.dev.rx.push_back(f);
                }
            }
        }

        fn tcp(&mut self, to: SocketAddrV4, local_port: u16) -> SocketHandle {
            self.tcp_to(SocketAddr::V4(to), local_port)
        }

        fn tcp_to(&mut self, to: SocketAddr, local_port: u16) -> SocketHandle {
            let mut s = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; 65536]),
                tcp::SocketBuffer::new(vec![0; 65536]),
            );
            s.connect(
                self.iface.context(),
                IpEndpoint::new(to.ip().into(), to.port()),
                local_port,
            )
            .unwrap();
            self.sockets.add(s)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guest_tcp_and_dns_through_the_gateway() {
        guest_through_the_gateway(false).await;
    }

    /// The same over a vz card's datagram socket: the policy and the
    /// stack are the gateway's either way.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vz_datagram_guest_through_the_gateway() {
        guest_through_the_gateway(true).await;
    }

    async fn guest_through_the_gateway(datagram: bool) {
        // An echo server on the host that the test policy allows (the
        // default one would refuse loopback).
        let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = server.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = server.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0; 4096];
                    while let Ok(n) = s.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        if s.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        let policy: Policy =
            Arc::new(move |ip, p| ip == IpAddr::V4(Ipv4Addr::LOCALHOST) && p == port);
        let gw = Gateway::start_with(policy, Options::default())
            .await
            .unwrap();
        let mut g = if datagram {
            #[cfg(unix)]
            {
                FakeGuest::attach_datagram(&gw).await
            }
            #[cfg(not(unix))]
            unreachable!()
        } else {
            FakeGuest::attach(&gw).await
        };

        // Allowed: an echo round trip, with more data than one window.
        let h = g.tcp(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port), 40000);
        let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let mut sent = 0;
        let mut got = Vec::new();
        for _ in 0..2000 {
            g.pump(5).await;
            let s = g.sockets.get_mut::<tcp::Socket>(h);
            if s.can_send() && sent < payload.len() {
                sent += s.send_slice(&payload[sent..]).unwrap();
            }
            while s.can_recv() {
                let mut b = vec![0; 65536];
                let n = s.recv_slice(&mut b).unwrap();
                got.extend_from_slice(&b[..n]);
            }
            if got.len() == payload.len() {
                break;
            }
        }
        assert_eq!(got.len(), payload.len(), "echoed everything");
        assert!(got == payload);

        // Refused: another port gets a reset.
        let h2 = g.tcp(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 1), 40001);
        g.pump(300).await;
        let s = g.sockets.get_mut::<tcp::Socket>(h2);
        assert_eq!(s.state(), tcp::State::Closed, "refused with a reset");

        // DNS: `localhost` resolves to loopback, which the guest may not
        // reach here except on the echo port, so it has no answer.
        let mut u = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
        );
        u.bind(5353).unwrap();
        let uh = g.sockets.add(u);
        g.sockets
            .get_mut::<udp::Socket>(uh)
            .send_slice(
                &query("localhost", 1),
                IpEndpoint::new(IpAddress::Ipv4(GATEWAY), DNS_PORT),
            )
            .unwrap();
        let mut reply = None;
        for _ in 0..200 {
            g.pump(10).await;
            let mut b = [0u8; 1500];
            if let Ok((n, _)) = g.sockets.get_mut::<udp::Socket>(uh).recv_slice(&mut b) {
                reply = Some(b[..n].to_vec());
                break;
            }
        }
        let r = reply.expect("a DNS reply");
        assert_eq!(&r[..2], &[0x12, 0x34]);
        assert_eq!(
            u16::from_be_bytes([r[6], r[7]]),
            0,
            "loopback is not handed out"
        );
    }

    fn v6_octets(ip: IpAddr) -> [u8; 16] {
        match ip {
            IpAddr::V6(v) => v.octets(),
            IpAddr::V4(_) => unreachable!(),
        }
    }

    fn spec(rules: &[&str], mbps: Option<u32>) -> NetSpec {
        NetSpec {
            allow: NetSpec::parse_list(rules).unwrap(),
            mbps,
        }
    }

    /// Names in a list open what DNS answered for them, for their ports
    /// only; the base policy still decides first.
    #[test]
    fn allow_lists_learn_from_dns() {
        let everything: Policy = Arc::new(|ip, _| ip != IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)));
        let g = Guard {
            base: everything,
            spec: spec(&["pypi.org:443", "*.example.com", "1.1.1.1:53"], None),
            learned: Default::default(),
        };
        let a: IpAddr = "151.101.0.223".parse().unwrap();
        let b: IpAddr = "93.184.216.34".parse().unwrap();
        assert!(!g.allows(a, 443), "nothing looked up yet");
        g.learn("pypi.org", &[a]);
        g.learn("evil.org", &[b]);
        assert!(g.allows(a, 443));
        assert!(!g.allows(a, 80), "only the rule's port");
        assert!(!g.allows(b, 443), "not on the list");
        g.learn("www.example.com", &[b]);
        assert!(g.allows(b, 8080), "every port");
        assert!(g.allows("1.1.1.1".parse().unwrap(), 53));
        assert!(!g.allows("1.1.1.1".parse().unwrap(), 443));
        g.learn("x.example.com", &["9.9.9.9".parse().unwrap()]);
        assert!(
            !g.allows("9.9.9.9".parse().unwrap(), 443),
            "the policy first"
        );
    }

    #[test]
    fn buckets_pace() {
        let b = Bucket::new(100_000);
        assert_eq!(b.take(10_000), Duration::ZERO, "within the burst");
        let wait = b.take(200_000);
        assert!(
            wait > Duration::from_millis(1500) && wait < Duration::from_secs(3),
            "{wait:?}"
        );
        assert!(!b.try_take(1), "in debt");
        let open = Meter::new(&NetSpec::default());
        assert!(open.try_send(1 << 30));
        assert_eq!(open.usage().tx_bytes, 1 << 30);
    }

    async fn udp_echo() -> u16 {
        let s = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = s.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            while let Ok((n, from)) = s.recv_from(&mut buf).await {
                let mut reply = b"echo:".to_vec();
                reply.extend_from_slice(&buf[..n]);
                let _ = s.send_to(&reply, from).await;
            }
        });
        port
    }

    async fn tcp_echo() -> u16 {
        let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = server.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = server.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0; 4096];
                    while let Ok(n) = s.read(&mut buf).await {
                        if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        port
    }

    /// UDP goes out and comes back through the gateway when the list
    /// allows it, and is counted; what the list leaves out is refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guest_udp_and_an_allow_list() {
        let uport = udp_echo().await;
        let tport = tcp_echo().await;
        // The base policy lets loopback through on both ports (the default
        // one would refuse it); the list takes the TCP port away again.
        let policy: Policy = Arc::new(move |ip, p| {
            ip == IpAddr::V4(Ipv4Addr::LOCALHOST) && (p == uport || p == tport)
        });
        let gw = Gateway::start_with(
            policy,
            Options {
                spec: spec(&[&format!("127.0.0.1:{uport}")], None),
                ipv6: false,
            },
        )
        .await
        .unwrap();
        let mut g = FakeGuest::attach(&gw).await;
        let mut u = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 8], vec![0; 8192]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 8], vec![0; 8192]),
        );
        u.bind(41000).unwrap();
        let uh = g.sockets.add(u);
        let to = IpEndpoint::new(IpAddress::Ipv4(Ipv4Addr::LOCALHOST), uport);
        let mut reply = None;
        for i in 0..300 {
            if i % 50 == 0 {
                // The first datagram may wait on ARP for the gateway.
                let _ = g
                    .sockets
                    .get_mut::<udp::Socket>(uh)
                    .send_slice(b"hello quic", to);
            }
            g.pump(10).await;
            let mut b = [0u8; 1500];
            if let Ok((n, meta)) = g.sockets.get_mut::<udp::Socket>(uh).recv_slice(&mut b) {
                assert_eq!(meta.endpoint, to, "the answer comes from the server");
                reply = Some(b[..n].to_vec());
                break;
            }
        }
        assert_eq!(reply.as_deref(), Some(&b"echo:hello quic"[..]));
        let used = gw.usage();
        // Each datagram sent is 10 bytes and each answer 15; one may still
        // be on its way.
        assert!(used.tx_bytes >= 10 && used.tx_bytes % 10 == 0, "{used:?}");
        assert!(used.rx_bytes >= 15 && used.rx_bytes % 15 == 0, "{used:?}");
        assert!(used.rx_bytes / 15 <= used.tx_bytes / 10, "{used:?}");

        // TCP to the port the list leaves out: a reset.
        let h = g.tcp(SocketAddrV4::new(Ipv4Addr::LOCALHOST, tport), 41001);
        g.pump(300).await;
        assert_eq!(
            g.sockets.get_mut::<tcp::Socket>(h).state(),
            tcp::State::Closed,
            "refused with a reset"
        );
    }

    /// A cap paces a TCP transfer, and both ways are counted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bandwidth_caps_pace_tcp() {
        let port = tcp_echo().await;
        let policy: Policy =
            Arc::new(move |ip, p| ip == IpAddr::V4(Ipv4Addr::LOCALHOST) && p == port);
        let gw = Gateway::start_with(
            policy,
            Options {
                // 1 Mbit/s: 125 kB a second each way.
                spec: spec(&[], Some(1)),
                ipv6: false,
            },
        )
        .await
        .unwrap();
        let mut g = FakeGuest::attach(&gw).await;
        let h = g.tcp(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port), 42000);
        let payload = vec![7u8; 200_000];
        let (mut sent, mut got) = (0, 0);
        let start = std::time::Instant::now();
        for _ in 0..4000 {
            g.pump(5).await;
            let s = g.sockets.get_mut::<tcp::Socket>(h);
            if s.can_send() && sent < payload.len() {
                sent += s.send_slice(&payload[sent..]).unwrap();
            }
            while s.can_recv() {
                let mut b = vec![0; 65536];
                got += s.recv_slice(&mut b).unwrap();
            }
            if got == payload.len() {
                break;
            }
        }
        assert_eq!(got, payload.len(), "echoed everything");
        let took = start.elapsed();
        // (200 kB - 32 kB of burst) at 125 kB/s.
        assert!(took > Duration::from_millis(1200), "{took:?}");
        let used = gw.usage();
        assert_eq!(used.tx_bytes, 200_000);
        assert_eq!(used.rx_bytes, 200_000);
    }

    /// With IPv6, the guest reaches IPv6 addresses through the gateway's
    /// own (neighbor discovery included).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guest_tcp_over_ipv6() {
        let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let local = server.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = server.accept().await {
                let _ = s.write_all(b"over v6").await;
            }
        });
        // A public-looking IPv6 destination (documentation range), which
        // the gateway's host socket reaches at the local listener.
        let dst = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x15);
        let port = 8080;
        REDIRECT
            .lock()
            .unwrap()
            .insert(SocketAddr::new(IpAddr::V6(dst), port), local);
        let policy: Policy = Arc::new(move |ip, p| ip == IpAddr::V6(dst) && p == port);
        let gw = Gateway::start_with(
            policy,
            Options {
                spec: NetSpec::default(),
                ipv6: true,
            },
        )
        .await
        .unwrap();
        assert!(gw.guest_net().v6.is_some());
        let mut g = FakeGuest::attach(&gw).await;
        let h = g.tcp_to(SocketAddr::new(IpAddr::V6(dst), port), 43000);
        let mut got = Vec::new();
        let mut state = tcp::State::Closed;
        for _ in 0..400 {
            g.pump(5).await;
            let s = g.sockets.get_mut::<tcp::Socket>(h);
            state = s.state();
            while s.can_recv() {
                let mut b = vec![0; 1024];
                let n = s.recv_slice(&mut b).unwrap();
                got.extend_from_slice(&b[..n]);
            }
            if got.len() >= 7 {
                break;
            }
        }
        assert_eq!(got, b"over v6", "guest socket {state}");
        assert_eq!(gw.usage().rx_bytes, 7);
    }
}
