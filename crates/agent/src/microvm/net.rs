//! Network for microVMs whose job asks for it (`network = true`).
//!
//! The guest gets a virtio-net card whose other end is not a host network
//! device but this gateway: QEMU sends the card's Ethernet frames over a
//! local TCP connection (`-netdev socket`), and a user-space TCP/IP stack
//! (smoltcp) terminates the guest's connections. The vz backend
//! (Virtualization.framework) hands its card's frames to a datagram socket
//! instead (`VZFileHandleNetworkDeviceAttachment`), which the vz helper
//! joins to the same kind of TCP connection ([`bridge_datagrams`]), so
//! both backends meet the same gateway and the same policy. For each TCP connection
//! the guest opens, the gateway opens one from the host to the same
//! address, if that address is allowed, and copies bytes both ways. DNS
//! queries to the gateway are answered with the host's resolver.
//!
//! What the guest can reach is decided here, on the host, by destination
//! address: public unicast IPv4 only. Private ranges (RFC 1918, CGNAT),
//! loopback, link-local (cloud metadata services), multicast, reserved
//! ranges and every address of the host itself are refused, as is SMTP
//! (port 25). Other UDP than DNS, ICMP and IPv6 are not forwarded. The
//! guest cannot get around this from inside: it only ever talks to the
//! gateway.
//!
//! Guest side: address [`GUEST`]/24, gateway and DNS server [`GATEWAY`].

use std::{
    collections::{HashMap, VecDeque},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use smoltcp::{
    iface::{Config, Interface, SocketHandle, SocketSet},
    phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken},
    socket::{tcp, udp},
    time::Instant as SmolInstant,
    wire::{
        EthernetAddress, EthernetFrame, EthernetProtocol, HardwareAddress, IpAddress, IpCidr,
        IpEndpoint, IpListenEndpoint, IpProtocol, Ipv4Packet, TcpPacket,
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
/// The guest card's MAC address (QEMU's default range).
pub const GUEST_MAC: &str = "52:54:00:12:34:56";
const GATEWAY_MAC: [u8; 6] = [0x52, 0x54, 0x00, 0x12, 0x34, 0x02];

/// Ethernet frame payload size (the guest's MTU).
const MTU: usize = 1500;
/// Largest frame accepted from QEMU (with the Ethernet header and room
/// for a VLAN tag).
const MAX_FRAME: usize = 65_536;
/// TCP buffers per connection on the guest side.
const TCP_BUFFER: usize = 256 * 1024;
/// Bytes read from a host connection but not yet taken by the guest.
const HOST_WINDOW: usize = 256 * 1024;
const CHUNK: usize = 64 * 1024;
const MAX_CONNECTIONS: usize = 512;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A connection the guest started but never completed.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long QEMU (or the vz helper) has to connect after the gateway
/// starts.
const ATTACH_TIMEOUT: Duration = Duration::from_secs(120);
const DNS_PORT: u16 = 53;
const MAX_DNS_IN_FLIGHT: usize = 64;
/// Ports never forwarded (outgoing mail would be spam from the provider's
/// address).
const BLOCKED_PORTS: &[u16] = &[25];

/// Whether a guest may connect to `ip` (public unicast IPv4, not this
/// host). See the module docs.
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

/// Whether `ip` is one of this machine's own addresses: only those can be
/// bound.
fn host_address(ip: Ipv4Addr) -> bool {
    std::net::UdpSocket::bind((ip, 0)).is_ok()
}

/// Decides which destinations the guest may reach.
pub type Policy = Arc<dyn Fn(Ipv4Addr, u16) -> bool + Send + Sync>;

/// The default policy: public addresses that are not this host's, and
/// not the blocked ports.
pub fn default_policy() -> Policy {
    let seen = std::sync::Mutex::new(HashMap::<Ipv4Addr, bool>::new());
    Arc::new(move |ip, port| {
        if BLOCKED_PORTS.contains(&port) || !public_v4(ip) {
            return false;
        }
        let mut seen = seen.lock().unwrap_or_else(|e| e.into_inner());
        *seen.entry(ip).or_insert_with(|| !host_address(ip))
    })
}

/// A running gateway for one VM. Dropping it ends every connection.
pub struct Gateway {
    /// Where QEMU connects (`-netdev socket,connect=...`), or the vz
    /// helper's [`bridge_datagrams`].
    pub addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Gateway {
    pub async fn start() -> Result<Self> {
        Self::start_with(default_policy()).await
    }

    pub async fn start_with(policy: Policy) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("listen for the VM's network card")?;
        let addr = listener.local_addr()?;
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
            if let Err(e) = serve(stream, policy).await {
                debug!("VM network ended: {e:#}");
            }
        });
        Ok(Self { addr, task })
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
type FlowKey = (u16, Ipv4Addr, u16);

/// A TCP SYN from the guest: its flow.
fn syn_flow(frame: &[u8]) -> Option<FlowKey> {
    let eth = EthernetFrame::new_checked(frame).ok()?;
    if eth.ethertype() != EthernetProtocol::Ipv4 {
        return None;
    }
    let ip = Ipv4Packet::new_checked(eth.payload()).ok()?;
    if ip.next_header() != IpProtocol::Tcp {
        return None;
    }
    let tcp = TcpPacket::new_checked(ip.payload()).ok()?;
    if !tcp.syn() || tcp.ack() {
        return None;
    }
    Some((tcp.src_port(), ip.dst_addr(), tcp.dst_port()))
}

async fn serve(stream: TcpStream, policy: Policy) -> Result<()> {
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
    });
    // Every destination routes through the gateway's own address, which
    // with any-IP makes smoltcp accept packets for any address.
    iface
        .routes_mut()
        .add_default_ipv4_route(GATEWAY)
        .map_err(|_| anyhow::anyhow!("no room for the default route"))?;
    iface.set_any_ip(true);
    let mut sockets = SocketSet::new(Vec::new());
    let dns = {
        let mut s = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 32], vec![0; 32 * 600]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 32], vec![0; 32 * 1500]),
        );
        s.bind(IpListenEndpoint {
            addr: Some(IpAddress::Ipv4(GATEWAY)),
            port: DNS_PORT,
        })
        .map_err(|e| anyhow::anyhow!("bind DNS: {e}"))?;
        sockets.add(s)
    };
    let dns_slots = Arc::new(Semaphore::new(MAX_DNS_IN_FLIGHT));
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Event>();
    let mut conns: HashMap<u64, Conn> = HashMap::new();
    let mut flows: HashMap<FlowKey, u64> = HashMap::new();
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
                    let s = sockets.get_mut::<udp::Socket>(dns);
                    let _ = s.send_slice(&answer, to);
                }
            }
        }

        for f in frames {
            if let Some(key) = syn_flow(&f)
                && !flows.contains_key(&key)
                && conns.len() < MAX_CONNECTIONS
                && policy(key.1, key.2)
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
                    addr: Some(IpAddress::Ipv4(key.1)),
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
                        SocketAddrV4::new(key.1, key.2),
                        from_guest,
                        credit.clone(),
                        ev_tx.clone(),
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
        loop {
            let s = sockets.get_mut::<udp::Socket>(dns);
            let Ok((n, meta)) = s.recv_slice(&mut buf) else {
                break;
            };
            let query = buf[..n].to_vec();
            let Ok(slot) = dns_slots.clone().try_acquire_owned() else {
                continue;
            };
            let ev = ev_tx.clone();
            let policy = policy.clone();
            tokio::spawn(async move {
                if let Some(answer) = resolve(&query, &policy).await {
                    let _ = ev.send(Event::Dns(meta.endpoint, answer));
                }
                drop(slot);
            });
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

        iface.poll(now(), &mut dev, &mut sockets);
        while let Some(f) = dev.tx.pop_front() {
            // A full queue drops the frame, as a busy link would; TCP
            // sends it again.
            let _ = out_tx.try_send(f);
        }
    }
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
    to: SocketAddrV4,
    mut from_guest: mpsc::Receiver<Vec<u8>>,
    credit: Arc<Semaphore>,
    ev: mpsc::UnboundedSender<Event>,
) {
    let stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(to)).await {
        Ok(Ok(s)) => s,
        _ => {
            let _ = ev.send(Event::Failed(id));
            return;
        }
    };
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let up = async move {
        while let Some(data) = from_guest.recv().await {
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

/// Answers a DNS query with the host's resolver: A records only (the
/// guest has no IPv6), and only addresses the guest may reach.
async fn resolve(query: &[u8], policy: &Policy) -> Option<Vec<u8>> {
    let q = parse_query(query)?;
    let addrs = if q.qtype == 1 && q.qclass == 1 {
        match tokio::time::timeout(
            Duration::from_secs(10),
            tokio::net::lookup_host((q.name.as_str(), 0)),
        )
        .await
        {
            Ok(Ok(it)) => Some(
                it.filter_map(|a| match a {
                    SocketAddr::V4(a) if policy(*a.ip(), 443) || policy(*a.ip(), 80) => {
                        Some(*a.ip())
                    }
                    _ => None,
                })
                .take(16)
                .collect::<Vec<_>>(),
            ),
            _ => None,
        }
    } else {
        Some(Vec::new())
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

/// The response to `query`: these A records, or NXDOMAIN for `None`.
fn answer(query: &[u8], q: &Query, addrs: Option<&[Ipv4Addr]>) -> Vec<u8> {
    let mut r = Vec::with_capacity(q.end + 16 * 16);
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
        // The name is the question's (a pointer to offset 12), type A,
        // class IN, a minute to live.
        r.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4]);
        r.extend_from_slice(&a.octets());
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
        let ip: Ipv4Addr = "151.101.0.223".parse().unwrap();
        let r = answer(&q, &parsed, Some(&[ip]));
        assert_eq!(&r[..2], &[0x12, 0x34]);
        assert_eq!(r[2] & 0x80, 0x80, "a response");
        assert_eq!(r[3] & 0x0f, 0, "no error");
        assert_eq!(u16::from_be_bytes([r[6], r[7]]), 1, "one answer");
        assert_eq!(&r[r.len() - 4..], &ip.octets());
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
            });
            iface.routes_mut().add_default_ipv4_route(GATEWAY).unwrap();
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
            let mut s = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; 65536]),
                tcp::SocketBuffer::new(vec![0; 65536]),
            );
            s.connect(
                self.iface.context(),
                IpEndpoint::new(IpAddress::Ipv4(*to.ip()), to.port()),
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
        let policy: Policy = Arc::new(move |ip, p| ip == Ipv4Addr::LOCALHOST && p == port);
        let gw = Gateway::start_with(policy).await.unwrap();
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
}
