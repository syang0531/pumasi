//! Network cards for Firecracker (1.18): a TAP device per VM, joined to
//! the same gateway as QEMU's and vz's cards ([`super::net`]).
//!
//! Firecracker's virtio-net cards only take a TAP device, which only root
//! (or `CAP_NET_ADMIN`) can make. An agent run as root on Linux makes one
//! per VM (`pmtapN`, persistent until the VM ends), gives it to
//! Firecracker by name, and reads and writes the card's Ethernet frames on
//! the host side with a packet socket bound to the device, passing them to
//! the gateway in QEMU's stream framing. The device has no address and
//! IPv6 is off on it, and the frames go to the gateway's MAC address, not
//! the device's, so the host's own network stack ignores them: what the
//! guest reaches is still only what the gateway allows.
//!
//! The guest turns its card's offloads off (`pumasi-guest`), so frames
//! arrive whole, with their checksums, as from QEMU.

use std::{
    io,
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tracing::{debug, trace};

const TUN_PATH: &str = "/dev/net/tun";
/// `_IOW('T', 202, int)`.
const TUNSETIFF: libc::c_ulong = 0x4004_54ca;
/// `_IOW('T', 203, int)`.
const TUNSETPERSIST: libc::c_ulong = 0x4004_54cb;
const IFF_TAP: libc::c_short = 0x0002;
const IFF_NO_PI: libc::c_short = 0x1000;
const IFF_VNET_HDR: libc::c_short = 0x4000;
/// Name prefix of the devices this agent makes.
const PREFIX: &str = "pmtap";

/// Whether this agent can give Firecracker VMs a network: root on Linux
/// with the TUN driver. Checked by making and removing a device.
pub fn available() -> bool {
    // SAFETY: no arguments, no side effects.
    if unsafe { libc::geteuid() } != 0 || !std::path::Path::new(TUN_PATH).exists() {
        return false;
    }
    match Tap::create() {
        Ok(_) => true,
        Err(e) => {
            debug!("no TAP devices for Firecracker: {e:#}");
            false
        }
    }
}

fn ifreq(name: &str) -> Result<libc::ifreq> {
    // SAFETY: a plain C struct; all zeroes is valid.
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    if name.len() >= ifr.ifr_name.len() {
        bail!("interface name {name:?} is too long");
    }
    for (i, b) in name.bytes().enumerate() {
        ifr.ifr_name[i] = b as libc::c_char;
    }
    Ok(ifr)
}

fn name_of(ifr: &libc::ifreq) -> String {
    let bytes: Vec<u8> = ifr
        .ifr_name
        .iter()
        .take_while(|c| **c != 0)
        .map(|c| *c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Opens the TUN driver and attaches to (or makes) the TAP device `name`
/// (`pmtap%d` picks a free number).
fn attach(name: &str, flags: libc::c_short) -> Result<(OwnedFd, String)> {
    let path = c"/dev/net/tun";
    // SAFETY: a valid C string; the fd is owned below.
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("open /dev/net/tun");
    }
    // SAFETY: just opened, and owned from here.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut ifr = ifreq(name)?;
    ifr.ifr_ifru.ifru_flags = flags;
    // SAFETY: TUNSETIFF takes a `struct ifreq`.
    if unsafe { libc::ioctl(fd.as_raw_fd(), TUNSETIFF as _, &mut ifr) } < 0 {
        return Err(io::Error::last_os_error()).context("TUNSETIFF");
    }
    Ok((fd, name_of(&ifr)))
}

fn control_socket() -> Result<OwnedFd> {
    // SAFETY: plain socket call; the fd is owned below.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("socket");
    }
    // SAFETY: just opened.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// A TAP device for one VM, removed on drop.
pub struct Tap {
    pub name: String,
    index: libc::c_int,
}

impl Tap {
    /// Makes a persistent TAP device, up, without IPv6, for Firecracker
    /// to attach to by name.
    pub fn create() -> Result<Self> {
        let (fd, name) = attach(&format!("{PREFIX}%d"), IFF_TAP | IFF_NO_PI | IFF_VNET_HDR)?;
        // SAFETY: TUNSETPERSIST takes an int.
        if unsafe { libc::ioctl(fd.as_raw_fd(), TUNSETPERSIST as _, 1 as libc::c_int) } < 0 {
            return Err(io::Error::last_os_error()).context("TUNSETPERSIST");
        }
        drop(fd);
        let mut tap = Self { name, index: 0 };
        let res = (|| -> Result<()> {
            // Nothing of the host's own stack on it: no IPv6 (router and
            // neighbor solicitations), no address.
            let _ = std::fs::write(
                format!("/proc/sys/net/ipv6/conf/{}/disable_ipv6", tap.name),
                "1",
            );
            let ctl = control_socket()?;
            let mut ifr = ifreq(&tap.name)?;
            // SAFETY: SIOCGIFFLAGS/SIOCSIFFLAGS take a `struct ifreq`.
            unsafe {
                if libc::ioctl(ctl.as_raw_fd(), libc::SIOCGIFFLAGS as _, &mut ifr) < 0 {
                    return Err(io::Error::last_os_error()).context("read the TAP flags");
                }
                ifr.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short;
                if libc::ioctl(ctl.as_raw_fd(), libc::SIOCSIFFLAGS as _, &ifr) < 0 {
                    return Err(io::Error::last_os_error()).context("bring the TAP device up");
                }
            }
            let c = std::ffi::CString::new(tap.name.clone())?;
            // SAFETY: a valid C string.
            tap.index = unsafe { libc::if_nametoindex(c.as_ptr()) } as libc::c_int;
            if tap.index == 0 {
                return Err(io::Error::last_os_error()).context("TAP device index");
            }
            Ok(())
        })();
        res.map(|()| tap)
    }

    /// A packet socket on the device: what the VM sends arrives there, and
    /// what is sent on it goes to the VM.
    fn packet_socket(&self) -> Result<OwnedFd> {
        let proto = (libc::ETH_P_ALL as u16).to_be();
        // SAFETY: plain socket call; the fd is owned below.
        let fd = unsafe {
            libc::socket(
                libc::AF_PACKET,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                proto as libc::c_int,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error()).context("packet socket");
        }
        // SAFETY: just opened.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: a plain C struct; all zeroes is valid.
        let mut sll: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
        sll.sll_family = libc::AF_PACKET as u16;
        sll.sll_protocol = proto;
        sll.sll_ifindex = self.index;
        // SAFETY: a `sockaddr_ll` of the right size.
        let r = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&sll as *const libc::sockaddr_ll).cast(),
                std::mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t,
            )
        };
        if r < 0 {
            return Err(io::Error::last_os_error()).context("bind the packet socket");
        }
        Ok(fd)
    }

    /// Joins the device to a [`super::net::Gateway`] on two threads, which
    /// end when either side closes (the gateway when the VM ends).
    pub fn bridge(&self, gateway: SocketAddr) -> Result<()> {
        use std::io::{Read, Write};
        let sock = self.packet_socket()?;
        let stream = std::net::TcpStream::connect_timeout(&gateway, Duration::from_secs(10))
            .with_context(|| format!("connect to the VM network gateway at {gateway}"))?;
        let _ = stream.set_nodelay(true);
        let mut from_gw = stream.try_clone()?;
        let mut to_gw = stream;
        // The reader wakes now and then to see whether the other side
        // ended.
        let timeout = libc::timeval {
            tv_sec: 0,
            tv_usec: 500_000,
        };
        // SAFETY: an open socket and a `timeval`.
        unsafe {
            libc::setsockopt(
                sock.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                (&timeout as *const libc::timeval).cast(),
                std::mem::size_of::<libc::timeval>() as libc::socklen_t,
            );
        }
        let rx = sock.as_raw_fd();
        let sock = std::sync::Arc::new(sock);
        let tx_sock = sock.clone();
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done2 = done.clone();
        std::thread::Builder::new()
            .name("tap-up".into())
            .spawn(move || {
                let _keep = sock;
                let mut buf = vec![0u8; super::net::MAX_FRAME];
                let mut out = Vec::with_capacity(4 + super::net::MAX_FRAME);
                loop {
                    if done.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    let n = match recv_incoming(rx, &mut buf) {
                        Ok(Some(n)) => n,
                        Ok(None) => continue,
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::Interrupted
                                    | io::ErrorKind::WouldBlock
                                    | io::ErrorKind::TimedOut
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => break,
                    };
                    out.clear();
                    out.extend_from_slice(&(n as u32).to_be_bytes());
                    out.extend_from_slice(&buf[..n]);
                    if to_gw.write_all(&out).is_err() {
                        break;
                    }
                }
            })?;
        std::thread::Builder::new()
            .name("tap-down".into())
            .spawn(move || {
                let mut frame = vec![0u8; super::net::MAX_FRAME];
                loop {
                    let mut len = [0u8; 4];
                    if from_gw.read_exact(&mut len).is_err() {
                        break;
                    }
                    let len = u32::from_be_bytes(len) as usize;
                    if len > super::net::MAX_FRAME || from_gw.read_exact(&mut frame[..len]).is_err()
                    {
                        break;
                    }
                    // SAFETY: a valid buffer of `len` bytes.
                    let r =
                        unsafe { libc::send(tx_sock.as_raw_fd(), frame.as_ptr().cast(), len, 0) };
                    if r < 0 {
                        trace!("frame to the VM dropped: {}", io::Error::last_os_error());
                    }
                }
                // The reader ends too.
                done2.store(true, std::sync::atomic::Ordering::Relaxed);
                drop(tx_sock);
            })?;
        Ok(())
    }
}

/// One frame the VM sent (`None` for the device's own outgoing traffic).
fn recv_incoming(fd: RawFd, buf: &mut [u8]) -> io::Result<Option<usize>> {
    // SAFETY: a plain C struct; all zeroes is valid.
    let mut sll: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t;
    // SAFETY: valid buffers and lengths.
    let n = unsafe {
        libc::recvfrom(
            fd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            0,
            (&mut sll as *mut libc::sockaddr_ll).cast(),
            &mut len,
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    if sll.sll_pkttype == libc::PACKET_OUTGOING {
        return Ok(None);
    }
    Ok(Some(n as usize))
}

impl Drop for Tap {
    fn drop(&mut self) {
        // Attach once more and clear the persist flag: the device goes
        // once Firecracker has let go of it.
        match attach(&self.name, IFF_TAP | IFF_NO_PI | IFF_VNET_HDR) {
            Ok((fd, _)) => {
                // SAFETY: TUNSETPERSIST takes an int.
                if unsafe { libc::ioctl(fd.as_raw_fd(), TUNSETPERSIST as _, 0 as libc::c_int) } < 0
                {
                    debug!(tap = %self.name, "TUNSETPERSIST 0: {}", io::Error::last_os_error());
                }
            }
            Err(e) => debug!(tap = %self.name, "remove the TAP device: {e:#}"),
        }
    }
}

/// Removes TAP devices an earlier agent left (it was killed with VMs
/// running).
pub fn remove_stale() {
    let Ok(rd) = std::fs::read_dir("/sys/class/net") else {
        return;
    };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(PREFIX) && name[PREFIX.len()..].bytes().all(|b| b.is_ascii_digit()) {
            drop(Tap { name, index: 0 });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As root (CI runs it with sudo): frames cross a TAP device and its
    /// packet socket both ways. The test plays Firecracker's part of the
    /// device; [`super::super::net`]'s tests cover the gateway itself.
    #[test]
    #[ignore = "needs root: sudo, then --ignored tap"]
    fn tap_frames_cross_both_ways() {
        assert!(available(), "root and /dev/net/tun");
        let tap = Tap::create().unwrap();
        // The gateway's side: a TCP listener speaking QEMU's framing.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        tap.bridge(addr).unwrap();
        let (mut gw, _) = listener.accept().unwrap();
        gw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        // The VM's side, as Firecracker opens it (without its vnet header).
        let (vm, _) = attach(&tap.name, IFF_TAP | IFF_NO_PI).unwrap();
        let mut frame = vec![0xffu8; 6];
        frame.extend_from_slice(&[0x52, 0x54, 0x00, 0x12, 0x34, 0x56]);
        frame.extend_from_slice(&[0x88, 0xb5]); // local experimental ethertype
        frame.extend_from_slice(b"from the guest, padded to sixty bytes.......");
        // SAFETY: a valid buffer.
        let n = unsafe { libc::write(vm.as_raw_fd(), frame.as_ptr().cast(), frame.len()) };
        assert_eq!(n as usize, frame.len());
        use std::io::{Read, Write};
        let mut len = [0u8; 4];
        gw.read_exact(&mut len).unwrap();
        let mut got = vec![0u8; u32::from_be_bytes(len) as usize];
        gw.read_exact(&mut got).unwrap();
        assert_eq!(got, frame, "the guest's frame reached the gateway");
        // And back.
        let mut back = vec![0x52, 0x54, 0x00, 0x12, 0x34, 0x56];
        back.extend_from_slice(&[0x52, 0x54, 0x00, 0x12, 0x34, 0x02, 0x88, 0xb5]);
        back.extend_from_slice(b"from the gateway, padded to sixty bytes.....");
        let mut msg = (back.len() as u32).to_be_bytes().to_vec();
        msg.extend_from_slice(&back);
        gw.write_all(&msg).unwrap();
        let mut buf = vec![0u8; 2048];
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            // SAFETY: a valid buffer.
            let n = unsafe { libc::read(vm.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            assert!(n > 0, "read the TAP device: {}", io::Error::last_os_error());
            if buf[..n as usize] == back[..] {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "the frame never came");
        }
        let name = tap.name.clone();
        drop(vm);
        drop(tap);
        assert!(
            !std::path::Path::new(&format!("/sys/class/net/{name}")).exists(),
            "the device is gone"
        );
    }
}
